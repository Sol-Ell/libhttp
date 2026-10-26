# libhttp conn — Design

**Status:** subsystem design, rewritten after verification. Owns the shared surface: the typed message model, the event vocabulary, the body handle, the send API, and the connection envelope for both TCP and QUIC.
**Parent:** `2026-10-26-libhttp-design.md`.
**Consumes:** transport (`ConnId`, `Chunk`, `ChunkMut`, `CreditSink`, `Read`, `Sent`, `TimerKind`, `CloseReason`), `tls` (`TlsConfig`, `tls::Error`).
**Consumed by:** `h1`, `h2`, `h3`, `proxy`, `dispatch`.

---

## 1. Scope

**Owns:** the typed model, `Event`, `Body`, `StreamId`/`StreamKind`/`Flow`/`OpenError`, the send API including tunnels and datagrams, the `Conn` envelope for both transports, protocol selection, output admission, `finish`/`Drained`, `tick`/`arm`, `on_deadline`, `on_closed`, and `ConnError`.

**Does not own:** sockets and readiness (transport), TLS mechanics (`tls`), frame and field encodings (each protocol), worker dispatch (`dispatch`).

---

## 2. The typed model

```rust
pub struct StreamId(u64);            // Copy + Eq + Hash + Ord; 0-based for h1, native for h2/h3
pub enum StreamKind { Request, Connect }

pub struct Method(u16, Option<Box<[u8]>>);
impl Method {
    pub const GET: Method;  pub const HEAD: Method;   pub const POST: Method;
    pub const PUT: Method;  pub const DELETE: Method; pub const OPTIONS: Method;
    pub const CONNECT: Method; pub const TRACE: Method; pub const PATCH: Method;
    pub fn from_bytes(b: &[u8]) -> Result<Method, MethodError>;
    pub fn as_str(&self) -> &str;                 // "OTHER" for an unknown token
}

pub struct StatusCode(u16);
impl StatusCode {
    pub fn as_u16(&self) -> u16;
    pub fn is_informational(&self) -> bool;
    pub fn canonical_reason(&self) -> Option<&'static str>;
}

pub enum Scheme { Http, Https, Other(Box<[u8]>) }
pub enum Target { Origin(Box<[u8]>), Absolute(Box<[u8]>), Authority(Box<[u8]>), Asterisk }
pub enum Version { Http10, Http11, Http2, Http3 }

pub enum HeaderName<'a> { Known(u16), Raw(&'a [u8]) }
impl<'a> HeaderName<'a> {
    pub const CONTENT_LENGTH: HeaderName<'static>;
    pub const HOST: HeaderName<'static>;
    pub const CONNECTION: HeaderName<'static>;
    pub const TRAILER: HeaderName<'static>;
    // ... the registered set the library itself uses
    pub fn as_bytes(&self) -> &[u8];
    pub fn eq_ignore_ascii_case(&self, other: HeaderName<'_>) -> bool;
}

pub struct HeaderValue<'a>(&'a [u8]);
impl<'a> HeaderValue<'a> {
    pub fn as_bytes(&self) -> &'a [u8];
    pub fn to_str(&self) -> Option<&'a str>;
    pub fn content_length(&self) -> Option<u64>;
    pub fn connection_tokens(&self) -> impl Iterator<Item = &'a [u8]> + '_;
}

pub struct StreamId(u64);   // declared above; kept here for the reader
```

### 2.1 Head

A head is one of three things, and the type says which:

```rust
pub enum HeadKind {
    Request { method: Method, target: Target, scheme: Scheme, version: Version },
    Response { status: StatusCode, version: Version },
    Trailers,
}

pub enum TrailerPolicy { Forbid, PreferChunked }

pub struct Head {
    arena: Chunk,               // owns the bytes the field spans point into
    kind: HeadKind,
    policy: TrailerPolicy,
    fields: Box<[FieldSpan]>,   // wire order, duplicates kept
}

pub struct FieldSpan { name: NameRepr, value: std::ops::Range<u32> }
pub enum NameRepr { Known(u16), Raw(std::ops::Range<u32>) }

impl Head {
    pub fn view(&self) -> HeadView<'_>;
}

pub struct HeadView<'a>(&'a Head);
impl<'a> HeadView<'a> {
    pub fn kind(&self) -> &HeadKind;
    pub fn fields(&self) -> Fields<'a, 'a>;
    pub fn trailer_policy(&self) -> TrailerPolicy;
}

pub struct Fields<'h, 'a>(/* ... */);       // read-only view over the arena
impl<'h, 'a> Fields<'h, 'a> {
    pub fn get(&self, name: HeaderName<'_>) -> Option<HeaderValue<'a>>;
    pub fn get_all(&self, name: HeaderName<'_>) -> impl Iterator<Item = HeaderValue<'a>> + '_;
    pub fn iter(&self) -> impl Iterator<Item = (HeaderName<'h>, HeaderValue<'a>)> + '_;
    pub fn len(&self) -> usize;
}
```

`Head` is immutable once built; construction is the builder's job, because a head assembled by parsing and a head assembled by an application have the same shape and different sources:

```rust
pub struct HeadBuilder { /* fields, kind, policy */ }
impl HeadBuilder {
    pub fn request(method: Method, target: Target, scheme: Scheme, version: Version) -> Self;
    pub fn response(status: StatusCode, version: Version) -> Self;
    pub fn trailers() -> Self;
    pub fn field(&mut self, name: HeaderName<'_>, value: HeaderValue<'_>) -> &mut Self;
    pub fn extend(&mut self, src: Fields<'_, '_>) -> &mut Self;
    pub fn trailer_policy(&mut self, policy: TrailerPolicy) -> &mut Self;
    pub fn build(self) -> Head;          // copies fields into one arena chunk
}
pub fn strip_hop_by_hop(out: &mut HeadBuilder, src: Fields<'_, '_>);   // proxy helper
```

`Head` owns its arena, so it survives the read chunk being recycled: one refcount, not one copy. Every `HeadView`/`Fields` borrows from the arena; nothing returns a span that outlives its `Head`.

`TrailerPolicy` is carried on the head because h1 must decide chunked framing **before** the response head is written (`h1` §4). It is ignored by h2 and h3, where trailers need no such decision.

`Fields` is read-only. A relay builds a new head rather than mutating the parsed one; `strip_hop_by_hop` is the one helper, and it filters while copying, so hop-by-hop fields never enter the new arena.

---

## 3. Events

```rust
pub enum Event {
    Head { stream: StreamId, head: Head },
    Data { stream: StreamId, body: Body },
    Trailers { stream: StreamId, head: Head },   // kind == HeadKind::Trailers; terminal
    End { stream: StreamId },                     // terminal, no trailers
    Reset { stream: StreamId, code: u64 },
    GoAway { last_stream: StreamId, code: u64 },
    Settings,
    Ready,
    Drained,
    StopAccepting,                                // the peer asked us to wind down
    Tunnel { stream: StreamId },
    StreamWritable { stream: StreamId },
    StreamsAvailable { count: usize },
    Datagram { flow: StreamId, data: Chunk },     // h3 RFC 9221; carry the Quarter Stream ID
    DatagramWritable,                             // h3: the datagram send buffer cleared
}
```

Ordering guarantees, enforced by the engine and checked by the fuzz targets:

- `Head` precedes any `Data` on that stream.
- `End` or `Trailers` is last on that stream; no `Data` follows either, and the two are mutually exclusive.
- `Trailers` carries `HeadKind::Trailers`; nothing else may.
- `Reset` or `GoAway` may follow anything; nothing follows on that stream after a `Reset`.
- `Ready` appears once, before any other event.
- `Drained` appears at most once, only after `finish` (§6.5).
- `Datagram` events are connection-level: they carry no stream id, because RFC 9221 datagrams are not stream frames.

---

## 4. The body handle

```rust
pub struct Body {
    chunk: Chunk,
    offset: u32,                 // the view's start inside `chunk`
    bytes: u32,                  // the view's length
    stream: StreamId,
    sink: Option<Arc<CreditSink>>,   // None on h3, where credit is the transport's
}
impl Body {
    pub fn bytes(&self) -> &[u8];        // &chunk[offset .. offset + bytes]
    pub fn len(&self) -> usize;
    pub fn stream(&self) -> StreamId;
    pub fn split_at(self, at: usize) -> (Body, Body);   // shares storage, offsets differ
}
```

`split_at` produces two views over one storage with different `offset`/`bytes`; both keep the same `stream` and the same sink. Consuming both returns credit once for the whole, because the sink counts bytes, not handles.

`Body.stream` must equal the `stream` on the event that carried it; the engine sets both, and `send_data(stream, body)` returns `ConnError::StreamMismatch` if they disagree. That is a checked invariant rather than a hope, because the alternative is credit landing on the wrong stream.

`Option<Arc<CreditSink>>` is `Some` only for h2, the one protocol whose flow control is a window this layer returns on consumption. h1 has no window — its backpressure is not reading — and h3's window belongs to the transport (`h3` §7), so both carry `None`. A sink is passed to the `Conn` only when it needs one (§6.1), which is why a `Conn` never needs a `Driver`.

Dropping the last view for a `(stream, bytes)` credits the sink. A sink whose connection is gone is closed and the credit is discarded rather than delivered to a reused id (transport §5.3).

---

## 5. The send API

```rust
pub enum Flow { Accepted, Blocked(Body) }
pub enum OpenError { WouldBlock, Refused, Closed }

impl Conn {
    pub fn open(&mut self, kind: StreamKind) -> Result<StreamId, OpenError>;
    pub fn send_head(&mut self, stream: StreamId, head: Head) -> Result<(), ConnError>;
    pub fn send_data(&mut self, stream: StreamId, body: Body) -> Result<Flow, ConnError>;
    pub fn send_end(&mut self, stream: StreamId) -> Result<(), ConnError>;
    pub fn send_trailers(&mut self, stream: StreamId, head: Head) -> Result<(), ConnError>;
    pub fn send_datagram(&mut self, flow: StreamId, data: Chunk) -> Result<(), ConnError>;  // h3 only
    pub fn max_datagram_size(&mut self, flow: StreamId) -> Option<usize>;      // h3 only
    pub fn reset(&mut self, stream: StreamId, code: u64) -> Result<(), ConnError>;
    pub fn goaway(&mut self, last_stream: StreamId, code: u64);
    pub fn stop_accepting(&mut self);                                          // first GOAWAY
    pub fn body_wanted(&mut self, stream: StreamId);                            // h1: 100-continue

    pub fn read_tunnel(&mut self, stream: StreamId, buf: &mut ChunkMut) -> Result<Read, ConnError>;
    pub fn write_tunnel(&mut self, stream: StreamId, data: Chunk) -> Result<Sent, ConnError>;
    pub fn end_tunnel(&mut self, stream: StreamId) -> Result<(), ConnError>;
}
```

- `open` returns `OpenError::WouldBlock` at the peer's stream limit and `Refused` if the peer will not accept the kind at all. On **h1** the engine assigns the next pipeline id and never returns `WouldBlock`; the application does not choose ids.
- `send_data` returns `Flow::Blocked(body)`: a body refused by the stream's reservation is the caller's to retry. Silently dropping it would be a lost body; buffering it inside the engine would make the reservation a lie.
- `send_end` and `send_trailers` are terminal and mutually exclusive. Sending either twice, or sending one after the other, is `ConnError::StreamDone`, and the stream is aborted — not the connection (§7).
- `reset` refuses with `StreamDone` when both sides have already finished, because a frame on a closed stream is a protocol violation (RFC 9113 §5.1) and the signature must be able to say so.
- `send_datagram` is associated with a request stream (`flow`), which becomes the Quarter Stream ID on the wire. It is `ConnError::DatagramTooLarge` above the per-flow `max_datagram_size`, `ConnError::DatagramsUnsupported` when the peer did not advertise support, never a silent drop, and the buffer-full case is `Event::DatagramWritable` rather than an error.
- Reset and GOAWAY codes are `u64` because h3's do not fit in 32 bits; h2's values are a subset and are passed through. `codes` holds the constants the library uses, so a relay never hardcodes `0x8` or `0x10c`.
- The tunnel trio is per stream on every protocol. On h1 the stream is the connection's single tunnel, and `end_tunnel` maps to the connection half-close; on h2/h3 it is `END_STREAM` on that stream and the connection is untouched.

---

## 6. The envelope

One type, two transports:

```rust
pub enum ConnHandle { Tcp(ConnId), Quic(h3::ConnId) }

pub enum Protocol { Http1, Http2, Http3, Detect, Alpn(&'static [Version]) }

impl Conn {
    pub fn new(handle: ConnHandle, protocol: Protocol,
               tls: Option<tls::TlsConfig>, sink: Option<Arc<CreditSink>>)
        -> Result<Conn, ConnError>;
    pub fn start(&mut self) -> Result<(), ConnError>;
    pub fn feed(&mut self, bytes: Chunk) -> Result<(), ConnError>;
    pub fn feed_eof(&mut self) -> Result<(), ConnError>;
    pub fn poll(&mut self) -> Option<Event>;
    pub fn output(&mut self) -> Option<Chunk>;
    pub fn finish(&mut self);
    pub fn on_closed(&mut self, reason: CloseReason) -> Result<(), ConnError>;
    pub fn tick(&mut self) -> Result<(), ConnError>;
    pub fn arm(&mut self, drv: &mut Driver) -> Result<(), ConnError>;
    pub fn on_deadline(&mut self, kind: TimerKind) -> Result<(), ConnError>;
    pub fn protocol(&self) -> Option<Version>;
}
```

### 6.1 Construction

`new` rejects invalid combinations rather than silently ignoring an argument: `Protocol::Http3` requires `ConnHandle::Quic` and `tls: None` (QUIC carries its own TLS); `Protocol::Alpn` requires a `TlsConfig`; `Protocol::Http1`/`Http2`/`Detect` require `ConnHandle::Tcp`. A mismatch is `ConnError::Config`.

`sink` is the transport's credit sink, `None` for QUIC. It is passed at construction because a `Conn` never touches a `Driver` while parsing; only `arm` does.

### 6.2 start

`start` begins whatever the transport needs: a TLS handshake's first flight on TCP, a no-op for cleartext and for QUIC (whose handshake belongs to the endpoint). It is idempotent.

### 6.3 feed, feed_eof, feed_datagram

`feed` decrypts if TLS is present and hands plaintext to the engine. It never calls back into the application and never yields events; parsing output is drained by `poll`. `feed_eof` hands end-of-input to TLS first, then the engine — that ordering is what separates a clean `close_notify` from a truncated close (`tls` §3.4).

On a QUIC connection the packet path belongs to `h3::Endpoint` (`h3` §3): it feeds datagrams into the connection and takes packets out through internal calls. There is no public `feed_datagram`/`output_datagram` on `Conn`, because two public owners for one packet path is how a packet gets sent twice. `feed` on a QUIC connection is `ConnError::Config`.

### 6.4 poll, output, and the admission rule

`poll` yields events in the order §3 guarantees. `output` yields framed, encrypted bytes ready for the transport's `send`. On QUIC the equivalent is `h3::Endpoint::output_datagram`.

`output` is not one FIFO, and without this rule the design deadlocks:

1. a stream's pending body is admitted only when that stream's window covers it;
2. control frames and QPACK encoder/decoder bytes are **never** gated by a request stream;
3. a header block is admitted only after the encoder instructions it references.

A single ordered queue that a blocked stream can fill would put an h2 `PING` ACK or a GOAWAY behind a zero window the peer itself set.

### 6.5 finish, stop_accepting, Drained

`stop_accepting` asks the peer to stop opening streams: h2 sends the first GOAWAY with `last_stream = 2^31 - 1`, h3 sends a GOAWAY, h1 marks the connection for close after the current exchange.

`finish` requests a drain. After it, `poll` yields `Drained` exactly once when **all three** hold:

- every stream is terminal (h1: the pipeline is empty);
- no output is eligible (§6.4);
- TLS has no queued records (`tls` §3.3).

Only then may the application call `drv.shutdown_write`, and only if it has no partial send outstanding (transport §4.5). A GOAWAY is not the last byte: for h2 the second GOAWAY, and for h3 the final GOAWAY, are emitted when the last stream is terminal or the `Drain` deadline fires — never before, because that would truncate the responses the GOAWAY acknowledged.

### 6.6 tick, arm, on_deadline

`tick` drains the `CreditSink` (when there is one) and returns the aggregate to the engine, which may enqueue output: an h2 `WINDOW_UPDATE`, or an h3 `ShouldTransmit`. It is why the pump calls `tick` before draining output.

`arm` upserts engine deadlines into the driver's heap: `Idle`, `ReadProgress`, `WriteProgress`, and `Drain` after `finish`. `on_deadline` dispatches a fired `TimerKind`; what a kind means is the engine's, but the mapping is fixed: a `ReadProgress` expiry mid-body is a reset, a `Drain` expiry after `finish` is a close, an `Idle` expiry on a keep-alive connection is a graceful finish. `TimerKind::Worker` never reaches a connection.

### 6.7 on_closed

When the transport reports `Closed`, the application calls `on_closed(reason)`. The engine releases its streams, drops retained credit into closed sinks, and produces no further events except the ones already queued. A connection that is closed without `on_closed` leaks its per-stream state; that is the application's contract to honour, and `Server` does it for you.

---

## 7. Errors

```rust
#[non_exhaustive]
pub enum ConnError {
    Transport(transport::Error),      // retryable ones are transient; see below
    Protocol(ProtocolError),          // fatal to the connection
    Tls(tls::Error),                  // alert or truncated close; fatal
    Config,                           // invalid Conn::new combination
    StreamDone,                       // terminal already sent, trailers after a length-framed head, or reset after both ends
    StreamMismatch,                   // Body.stream != the stream named by send_data
    StreamRefused,                    // the peer will not accept this kind of stream
    DatagramTooLarge,
    DatagramsUnsupported,            // the peer did not advertise RFC 9221 support
    Closed,                           // the connection is already gone
}

pub struct ProtocolError {
    pub connection: bool,             // fatal to the connection, or only to the stream
    pub status: Option<StatusCode>,   // h1's 400/431/501/417, when one can still be sent
    pub code: u64,                    // h2/h3 error code, 0 otherwise
}
```

Fatality, which is the part callers branch on:

| Variant | Effect |
|---|---|
| `Protocol`, `Tls`, `Config` | the connection is dead |
| `Transport(Error::Io)`, `Transport(Error::StaleId)`, `Closed` | the connection is dead |
| `Transport(Error::Exhausted)` | transient: the pool is full, stop reading and retry |
| `StreamDone`, `StreamMismatch`, `StreamRefused`, `DatagramTooLarge`, `DatagramsUnsupported` | the stream (or the datagram) is refused; the connection survives |

h1's "response body written after End" aborts the stream; it closes the connection only because the response was already on the wire, and that is `h1`'s decision, not this variant's fatality.

---

## 7a. Codes

```rust
pub mod codes {
    pub const CANCEL: u64 = 0x8;                 // h2
    pub const REFUSED_STREAM: u64 = 0x7;         // h2
    pub const INTERNAL_ERROR: u64 = 0x2;         // h2
    pub const H3_REQUEST_REJECTED: u64 = 0x10b;  // h3
    pub const H3_REQUEST_CANCELLED: u64 = 0x10c; // h3
    pub const CONNECT_ERROR: u64 = 0xa;          // h2
    pub const H3_CONNECT_ERROR: u64 = 0x10f;      // h3
}
```

## 8. What a builder decides

The `Method` id table beyond the constants, the registered-name table, `Fields`' inline capacity, the field-span width (32 vs 64 bit), and `Target` validation strictness per form.

Not theirs: arena ownership, `TrailerPolicy` on the head, the checked `Body.stream` invariant, the admission rule, `Drained`'s three conditions, the `ProtocolError` fatality split, and `on_closed`.
