# libhttp conn — Design

**Status:** subsystem design. Owns the surface every protocol engine shares: the event vocabulary, the typed message model, the send API, protocol selection, and the graceful drain the transport explicitly does not own.
**Parent:** `2026-10-26-libhttp-design.md`.
**Consumes:** transport (`Driver`, `Chunk`, `ChunkMut`, `CreditSink`, `TimerKind`, `CloseReason`).
**Consumed by:** `tls`, `h1`, `h2`, `h3`, `proxy`.

---

## 1. Scope

**Owns:** the `Conn` envelope (TLS + one engine), `Event`, `Head`/`Fields`/`HeaderName`/`HeaderValue`, `Body`, `StreamId`/`StreamKind`, `Flow`, `ConnError`, protocol selection, output admission, `finish`/`Drained`, `tick`, `arm`, deadline dispatch.

**Does not own:** sockets and readiness (transport), TLS configuration and handshake mechanics (`tls`), frame/field encodings (per protocol), worker dispatch (`dispatch`).

---

## 2. The typed message model

```rust
pub struct Head {
    arena: Chunk,                 // owns the bytes the message was parsed from
    span: std::ops::Range<u32>,   // the message head inside `arena`
    fields: Box<[FieldSpan]>,     // one per field, in wire order, duplicates kept
    method: Method,
    target: Target,
    version: Version,
}
impl Head { pub fn view(&self) -> HeadView<'_>; }

pub struct HeadView<'a>(&'a Head);
impl HeadView<'_> {
    pub fn method(&self) -> Method;
    pub fn target(&self) -> &Target;
    pub fn version(&self) -> Version;
    pub fn fields(&self) -> Fields<'_, '_>;
}

pub struct Fields<'h, 'a>(/* ... */);
impl<'h, 'a> Fields<'h, 'a> {
    pub fn get(&self, name: HeaderName<'_>) -> Option<HeaderValue<'a>>;       // first
    pub fn get_all(&self, name: HeaderName<'_>) -> impl Iterator<Item = HeaderValue<'a>> + '_;
    pub fn iter(&self) -> impl Iterator<Item = (HeaderName<'h>, HeaderValue<'a>)> + '_;
    pub fn len(&self) -> usize;
    pub fn push(&mut self, name: HeaderName<'h>, value: HeaderValue<'a>);      // for responses
    pub fn strip_hop_by_hop(&mut self);
}

pub enum HeaderName<'a> { Known(u16), Raw(&'a [u8]) }
impl<'a> HeaderName<'a> {
    pub const CONTENT_LENGTH: HeaderName<'static>;
    pub const HOST: HeaderName<'static>;
    // ... the registered set the library uses
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

pub struct Method(u16, Option<Box<[u8]>>);   // known ids, else the raw token
pub enum Version { Http10, Http11, Http2, Http3 }
pub enum Target { Origin(Box<[u8]>), Absolute(Box<[u8]>), Authority(Box<[u8]>), Asterisk }
```

`Head` owns its arena, so it survives the read chunk being recycled — that is one refcount, not one copy. `HeadView` and `Fields` are reconstructed per access and borrow from the arena; nothing in the API returns a span that outlives its `Head`.

`HeaderName` is an interned identity: registered names resolve to an id at compile time, unknown names are a validated byte range compared case-insensitively. Extension headers are first-class without becoming enum variants, which is what lets a relay forward `x-forwarded-for` byte-exactly.

`Fields` preserves order and duplicates; `strip_hop_by_hop` is a method over the typed list, not a string set.

---

## 3. The event vocabulary

```rust
pub enum Event {
    Head { stream: StreamId, head: Head },
    Data { stream: StreamId, body: Body },
    Trailers { stream: StreamId, fields: Head },   // a field-only block; terminal
    End { stream: StreamId },                      // body finished, no trailers
    Reset { stream: StreamId, code: u32 },
    GoAway { last_stream: StreamId, code: u32 },
    Settings,
    Ready,
    Drained,
    Tunnel { stream: StreamId },
    StreamWritable { stream: StreamId },
    StreamsAvailable { count: usize },
}
```

Ordering guarantees, enforced by the engine and checked by the fuzz targets:

- `Head` precedes any `Data` on that stream.
- `End` or `Trailers` is last on that stream; no `Data` follows either.
- `Reset` or `GoAway` may follow anything; nothing follows on that stream.
- `Ready` appears once, before any other event.
- `Drained` appears at most once, and only after `finish` (§5).

`Trailers` and `End` are different messages, not a convenience: a body that ends plainly is not an empty trailer block, and h3 distinguishes them.

---

## 4. The body handle

```rust
pub struct Body {
    chunk: Chunk,
    stream: StreamId,
    bytes: u32,              // bytes this handle is responsible for
    sink: Arc<CreditSink>,   // credits the source connection when this handle is consumed
}
impl Body {
    pub fn bytes(&self) -> &[u8];
    pub fn len(&self) -> usize;
    pub fn split_at(self, at: usize) -> (Body, Body);   // shares storage, no copy
}
```

A `Body` is the unit of flow control, not the chunk. Splitting yields two handles over one storage, each carrying its own `bytes`; consuming both returns the sum of credit. `send_data` consumes the handle, which is how a relay's credit travels from the source stream to the destination stream.

Dropping the last handle for `(stream, bytes)` credits the sink. A sink whose connection is gone is closed, and the credit is discarded rather than delivered to a reused id (transport §5.3).

---

## 5. The send API

```rust
pub enum StreamKind { Request, Connect }
pub enum Flow { Accepted, Blocked(Body) }

impl Conn {
    pub fn open(&mut self, kind: StreamKind) -> Result<StreamId, WouldBlock>;
    pub fn send_head(&mut self, stream: StreamId, head: Head) -> Result<(), ConnError>;
    pub fn send_data(&mut self, stream: StreamId, body: Body) -> Result<Flow, ConnError>;
    pub fn send_end(&mut self, stream: StreamId) -> Result<(), ConnError>;
    pub fn send_trailers(&mut self, stream: StreamId, fields: Head) -> Result<(), ConnError>;
    pub fn reset(&mut self, stream: StreamId, code: u32);
    pub fn goaway(&mut self, last_stream: StreamId, code: u32);
}
```

`Flow::Blocked` returns the body rather than consuming it. A body refused by the stream's reservation is the caller's to retry; silently dropping it would be a lost body, and buffering it inside the engine would make the reservation a lie.

`open` returns `WouldBlock` when the peer's stream limit is reached; `StreamsAvailable` says when to try again. There is no "open without a limit" path.

`send_end` and `send_trailers` are both terminal and mutually exclusive per stream; sending either twice is `ConnError::StreamDone`.

---

## 6. The connection envelope

```rust
pub enum Protocol { Http1, Http2, Detect, Alpn(&'static [Version]) }

impl Conn {
    pub fn new(id: ConnId, protocol: Protocol, tls: Option<TlsConfig>) -> Result<Conn, ConnError>;
    pub fn start(&mut self) -> Result<(), ConnError>;
    pub fn feed(&mut self, bytes: Chunk) -> Result<(), ConnError>;
    pub fn feed_eof(&mut self) -> Result<(), ConnError>;
    pub fn poll(&mut self) -> Option<Event>;
    pub fn output(&mut self) -> Option<Chunk>;
    pub fn finish(&mut self);
    pub fn tick(&mut self, drv: &mut Driver) -> Result<(), ConnError>;
    pub fn arm(&mut self, drv: &mut Driver) -> Result<(), ConnError>;
    pub fn on_deadline(&mut self, kind: TimerKind) -> Result<(), ConnError>;
    pub fn protocol(&self) -> Option<Version>;
}
```

### 6.1 Protocol selection

`Detect` buffers at most 24 bytes and either matches the HTTP/2 preface or commits to h1. `Alpn` creates the engine when the handshake resolves it. `poll` yields `Ready` once, when an engine exists and the application may send; `protocol()` reports the choice.

The invariant that makes this cheap: **no application bytes exist before the protocol is known.** Over TLS that holds because 0-RTT is out of scope; in cleartext it holds because `Detect` buffers 24 bytes. There is no plaintext holding pen.

### 6.2 feed and feed_eof

`feed` takes a chunk, decrypts if TLS is present, and hands plaintext to the engine. It never calls back into the application and never yields events; parsing output is drained by `poll` afterwards. Calling `feed` from inside `poll` is a borrow error, not a documented precondition.

`feed_eof` is the input path for the end of the byte stream, and it is the only way a truncated TLS close or a chunked body cut at FIN is detectable. The engine sees EOF before `poll` yields the terminal event for any stream still open.

### 6.3 poll

`poll` yields events in the order §3 guarantees. It never yields frames, HPACK/QPACK instructions, or partial parse state.

### 6.4 output and its admission rule

`output` yields framed, encrypted bytes ready for the transport's `send`. It is not one FIFO, and without the rule below the design deadlocks:

1. a stream's pending body is admitted only when that stream's window covers it;
2. control frames and QPACK encoder/decoder bytes are **never** gated by a request stream;
3. a header block is admitted only after the encoder instructions it references.

A single ordered queue that a blocked stream can fill would put an h2 `PING` ACK or a GOAWAY behind a zero window the peer itself set. With QPACK at capacity zero there are no encoder instructions to order today, but the rule is stated because the dynamic table lands at M5.

`output` returns `None` only when nothing is eligible. There is no "nothing right now but something later" state that the caller must poll for: a later window update arrives as input and produces output, and the application learns of it because `feed` marks the connection active.

### 6.5 finish, Drained, and the handover to the transport

`finish` sets an intent; it does not close anything. After it, the connection produces its final output (`close_notify`, h2 GOAWAY), and `poll` yields `Drained` once when all three hold:

- every stream is terminal (or h1's pipeline is empty);
- no output is eligible;
- TLS has no queued records.

Then, and only then, may the application call `drv.shutdown_write`. The precondition on the application side — no partial send outstanding — is the transport's (§4.5 there) and is not repeated here.

For h2, `finish` does **not** mean "send GOAWAY and stop": in-flight streams still have bodies. GOAWAY is emitted when the last stream is terminal or the `Drain` deadline fires, whichever comes first. A design that emits GOAWAY and half-closes immediately truncates the responses the GOAWAY just acknowledged.

### 6.6 tick, arm, deadlines

`tick` drains the connection's `CreditSink` and returns the aggregate to the engine, which may enqueue output (an h2 `WINDOW_UPDATE`, an h3 `ShouldTransmit`). It is why the pump calls `tick` before draining output.

`arm` upserts the engine's deadlines into the driver's heap: `Idle`, `ReadProgress` and `WriteProgress` as the engine decides; `Drain` after `finish`.

`on_deadline` dispatches a fired `TimerKind`. What a kind means is the engine's business: `ReadProgress` firing mid-body is a reset, `Drain` firing after `finish` is a close, `Idle` firing on a keep-alive connection is a graceful finish. `TimerKind::Worker` never reaches a connection — it is the dispatcher's.

### 6.7 feed_eof, close reasons, and truncation

`feed_eof` hands end-of-input to TLS first, then the engine. That ordering is what separates the two endings:

- a TLS connection that receives `close_notify` and then EOF is a clean peer close;
- a TLS connection that receives EOF with no `close_notify` reports a truncated close, which is an error.

The connection does not choose the transport's `CloseReason`; the transport does (transport §4.6). The connection's job is to stop producing a clean `End` when the input ended mid-message.

---

## 7. Errors

```rust
#[non_exhaustive]
pub enum ConnError {
    Transport(transport::Error),
    Protocol(ProtoError),        // an engine-level violation; the connection is dead
    StreamDone,                  // send after End/Trailers, or a second terminal
    StreamRefused,               // open/reset against a stream the peer will not accept
    Tls(tls::Error),             // alert or truncated close
    Stale,                       // the Conn's id is no longer live
}
```

`Protocol` is fatal: the connection is closed after it is reported. `StreamDone` and `StreamRefused` are per-stream and non-fatal. There is no `Box<dyn Error>` and no stringly-typed variant.

---

## 8. What a builder decides

The `Method` id table and the known-name table's exact contents, the `Fields` inline capacity, the `Target` validation strictness for each form, and whether `Head`'s field spans are 32- or 64-bit.

Not theirs: arena ownership, the event ordering guarantees, the admission rule, `Drained`'s three conditions, credit travelling on the body handle, and the send API's outcomes.
