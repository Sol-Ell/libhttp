# libhttp — Design

**Status:** draft for approval. **Date:** 2026-10-26.
**Supersedes:** `docs/superpowers/specs/2026-10-26-libhttp-requirements.md` §2 ("Low-level API is
public"), §5 proxy scope **and all CONNECT/`101`/half-close items (h1 and h2 plain CONNECT)**, §6
low-level type expectations, §9 ("Low-level public API; proxy is the acid test"), the public-API
framing of §10.1/§10.10, and §4's zero-copy body-forwarding/large-write/file-body goals as v1
features. Those §4 goals are deferred, not abandoned: the copy budget is documented in the runtime
design, and §2.2 states the deferrals. Everything else in the brief — intent, protocol scope minus
proxy, portability, performance, and the §8 failure lessons — remains binding. No archived design and
no prior git revision was read.

**Document map:** this file is the top-level design: scope, public API, the twelve §10 decisions,
performance gates, packaging, milestones and risks. The binding contracts live in four companion
documents, all in this directory:

| Document | Binds |
|---|---|
| [runtime design](2026-10-26-libhttp-runtime-design.md) | reactor loop, transports, TLS, buffers/head storage, copy/atomic/syscall budgets, timers, sockets, listeners, close/drain, memory arithmetic, fast-path scope |
| [execution design](2026-10-26-libhttp-execution-design.md) | worker pool, exchanges, queues, admission, deadline ownership, parking caps, per-stream isolation, cancellation, shutdown, limit reactions |
| [protocol design](2026-10-26-libhttp-protocols-design.md) | HTTP/1.1, HTTP/2, HTTP/3 wire behavior, parser strictness, HPACK/QPACK, flow control, QUIC backend choice, security tests |
| [client design](2026-10-26-libhttp-client-design.md) | client API, pool/queue semantics, DNS, retries, timeouts, limits |

---

## 0. The shape in one paragraph

`libhttp` is a Rust library for HTTP/1.1, HTTP/2 and HTTP/3 servers and clients. The public API is
high-level and synchronous: implement `Handler::handle(&mut Request, &mut Response)`, the library runs
it on its own bounded worker pool, and bodies are read and written through blocking, deadline-aware
handles. There is no async runtime, `Future`, `Waker` or executor anywhere, including TLS and QUIC.
Reactor threads own sockets, timers, buffers and protocol state machines and never block on a socket;
worker threads run handlers and block only on bounded library queues or their own I/O. Everything
below `Server` and `Client` is private. Protocol engines are sans-I/O byte machines shared by all
platforms; the transport layer is native per platform (epoll, kqueue, IOCP). Costs are bounded and
documented; they are measured by the harness, not exposed as public per-operation counters.

---

## 1. Intent and why

The library exists because the author wants HTTP servers and clients whose performance can be
reasoned about without an async runtime. The complaint is not abstraction as such: it is that existing
stacks force an abstraction and hide copies, allocations and wakeups. This design keeps the
high-level API that makes applications short and moves cost accounting into the design and the
benchmark harness rather than into the type system.

- A handler is an ordinary function. If it blocks on a database, it blocks one worker, not a reactor
  and not another connection.
- Every queue and buffer has a bound, and every bound has a reaction: backpressure, a typed error, a
  stream reset, or connection close.
- HTTP/1.1, HTTP/2 framing/HPACK, and HTTP/3 framing/QPACK are ours to test and fuzz; smuggling and
  malformed input are our responsibility.
- The chosen OS primitive is not the product. A platform without a fast path is slower, not
  different, and never has a different public API.

---

## 2. Scope

### 2.1 In scope for v1

- HTTP/1.1 server and client: parsing/serialization, chunked bodies, trailers, `100-continue`,
  keep-alive, pipelining with ordered responses, HTTP/1.0 behavior, strict framing and smuggling
  defenses.
- HTTP/2 server and client: frames, stream state, SETTINGS/GOAWAY/RST_STREAM, flow control, full
  HPACK, pseudo-headers, trailers, content-length checking, h2c prior knowledge, ALPN.
- HTTP/3 server and client: QUIC delegated to a sans-I/O transport (`quinn-proto` in v1), our HTTP/3
  framing, control streams, request streams, SETTINGS/GOAWAY, static-table QPACK.
- TLS for HTTP/1.1 and HTTP/2 via `rustls`; ALPN; `close_notify`; server/client certificates; client
  roots.
- Synchronous handlers on a bounded worker pool; streaming request and response bodies in server and
  client; typed errors; timeouts; cancellation; graceful shutdown.
- Linux, macOS/BSD, Windows; optional platform fast paths.

### 2.2 Out of scope for v1 (deferred, not rejected forever)

- Reverse proxy; public low-level API (driver/session/completion/socket-table types).
- CONNECT tunnels, `101 Switching Protocols`, WebSocket/Upgrade, extended CONNECT (RFC 8441).
- HTTP/2 push and priority; HTTP/3 push and HTTP datagrams.
- HTTP/3 0-RTT, connection migration, Alt-Svc. Happy Eyeballs is in the client's scope.
- A file-bodied-response API (`Response::file`); handlers stream files themselves.
- io_uring, multishot recv/accept, provided buffers, `SEND_ZC`, `sendfile`/`splice`,
  `MSG_ZEROCOPY`, Windows RIO/IoRing, kTLS, large-pages tuning.
- Dynamic-table QPACK, SNI certificate switching, TLS session-ticket rotation policy, native
  platform certificate verification, `aws-lc-rs`/FIPS providers.
- Automatic compression, content negotiation, routing, extractors, middleware, redirects, cookies.

### 2.3 Anti-goals

- A handler blocking on a socket or body read stalls a reactor or another connection.
- Any hidden executor, `Future`, `Waker`, or async runtime dependency in the published crate.
- Unbounded queues, unbounded per-connection buffering, or unbounded handler admission.
- Thread-per-connection.
- Panics, hangs, or unbounded memory on malformed input.
- A public API whose shape depends on epoll/kqueue/IOCP, `fd`, or a completion/readiness choice.
- Header storage that loses order or duplicates, or a `HashMap<String, String>` on the request path.
- Shipping a fast path that does not beat the portable path on the published benchmark.
- Claiming "zero copy" or "no atomics" where the common path copies or uses atomics.

---

## 3. Constraints

- Rust edition 2024; MSRV **1.85.0** (edition 2024 and `quinn-proto` 0.11.19 both require 1.85).
- No `Future`/`Waker`/executor/async runtime in the product; no `tokio`, `async-std`, `hyper`, or
  `h2` in the published dependency graph. They appear only in `bench/` and differential tooling.
- No blocking socket I/O on reactor threads; all sockets nonblocking (overlapped + IOCP on Windows).
- Worker threads may block on bounded library queues and on the application's own I/O; never on a
  socket.
- No thread per connection.
- Every queue and buffer bounded by configuration, with a defined reaction.
- Public API identical on all supported platforms; backend selection is compile-time per platform plus
  runtime probes for optional accelerators.
- `bytes` implementations are not the core buffer; `quinn-proto`'s internal `Bytes` may be wrapped
  privately and never appears in the public API. `httparse` is not used. `h2`/`h3` are not used in
  the product; `hyper`/`tokio` are dev/bench only.
- TLS is `rustls` (sans-I/O), used **only if it meets the TLS gate** (§8); QUIC is delegated to
  `quinn-proto`, which is a choice on merit, not an obligation (§7 table row 6, protocols §4). HTTP/3 framing and QPACK are
  ours.

---

## 4. Approaches considered

With the low-level API hidden, no public contract requires a uniform I/O model, so the only question
is where the portability seam sits.

**A. Sans-I/O engines, native transport per platform (chosen).** HTTP/1.1, HTTP/2, HTTP/3, HPACK,
QPACK and TLS are byte-to-byte state machines with no I/O and no threads — the shape `rustls` and
`quinn-proto` already impose. A small transport per platform owns sockets and buffers: epoll/kqueue
perform the syscall on readiness; IOCP keeps one overlapped receive posted and delivers completions.
Both present engines with byte-flow events. Engines are driven by byte scripts in tests and fuzzers
without sockets; io_uring later is another transport, not a second engine. Cost: three transports and
one explicit buffer-handoff contract.

**B. One completion-shaped driver on every platform, readiness emulated (rejected).** A uniform
op/completion contract closest to io_uring, native on IOCP and synthesized on epoll/kqueue. It buys a
single cancellation/op-id model, but makes Linux/macOS pay for machinery that exists to serve a
phase-2 backend, and adds reentrancy and buffer-ownership rules the byte-flow seam does not need. With
no public low-level API, the uniformity buys users nothing visible.

**C. Per-platform loops with duplicated protocol code (rejected).** Best fit per OS, least
abstraction, but three copies of framing and dispatch, guaranteed drift, and fragmented fuzz coverage.

**Decision: A.** The engine/transport seam is byte-flow; completion versus readiness is an internal
transport detail; the public API exposes neither.

---

## 5. Architecture at a glance

```text
                  ┌─────────────── library-owned worker pool ───────────────┐
                  │  Handler::handle(&mut Request, &mut Response)           │
                  └───────▲──────────────────────────────────┬──────────────┘
                          │ per-exchange inbound/outbound    │ condvar + one
                          │ rings, bounded, deadline-aware   │ coalesced wake
┌─────────────────────────┴──────────────────────────────────▼──────────────┐
│ Reactor thread                                                             │
│  socket slab + generations      timer heap (single owner)                  │
│  block pool + cross-thread returns                                         │
│                                                                            │
│  ┌────────────┐   ┌──────────────┐   ┌──────────────────────────────────┐  │
│  │ HTTP/1.1   │   │ TLS (rustls) │   │ TCP transport: epoll | kqueue |  │  │
│  │ HTTP/2     │◄──┤              │◄──┤ IOCP (one recv/one send posted)   │  │
│  │ engines    │   │              │   │                                  │  │
│  └────────────┘   └──────────────┘   └──────────────────────────────────┘  │
│  ┌───────────────────────────┐      ┌──────────────────────────────────┐  │
│  │ HTTP/3 engine + QPACK      │◄────┤ UDP transport + quinn-proto      │  │
│  └───────────────────────────┘      │ endpoint (batching on Linux)     │  │
│                                     └──────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────┘
```

Threads created by the library, all named, no global state: `libhttp-reactor-N` (socket ownership,
engines, timers, dispatch), `libhttp-worker-N` (handlers), `libhttp-client-reactor-N` (client
reactors). A process may build several `Server`/`Client` instances; nothing is shared unless the
application shares it.

---

## 6. Public API

Everything is in `libhttp` unless a module path is shown. Bodies, private fields and module layout
are builder discretion; signatures and semantics are the contract.

### 6.1 Surface map

| Module | Contents |
|---|---|
| `libhttp` | `Method`, `StatusCode`, `Version`, `HeaderName`, `HeaderValue`, `HeaderRef`, `HeaderMap`, `Header`, `HeaderError`, `Timeouts`, `Target`, `Uri`, `header`, `Error`, `TimeoutKind`, `ProtocolError`, `ProtocolErrorKind`, `TlsError` |
| `libhttp::server` | `Server`, `ServerBuilder`, `ServerHandle`, `Handler`, `Request`, `Response`, `DeferredRequest`, `DeferredResponse`, `BodyReader`, `BodyWriter`, `BodyChunk`, `Limits`, `Stats`, and a `Timeouts` re-export |
| `libhttp::client` | `Client`, `ClientBuilder`, `ClientHandle`, `ClientCall`, `CancelHandle`, `Executor`, `ClientRequest`, `ClientResponse`, `ClientStats`, `PoolConfig`; uses the root `Timeouts` |
| `libhttp::tls` | `TlsServerConfig`, `TlsClientConfig` (PEM/roots/ALPN/client-auth); rustls types stay internal in v1 |

### 6.2 Server and handler

```rust
pub trait Handler: Send + Sync + 'static {
    fn handle(&self, req: &mut Request, res: &mut Response) -> Result<(), Error>;
}
impl<F> Handler for F
where F: Fn(&mut Request, &mut Response) -> Result<(), Error> + Send + Sync + 'static {}

let server = Server::builder()
    .bind("0.0.0.0:8080")?           // repeatable; port 0 allowed
    .workers(8)                      // default: available_parallelism()
    .reactors(2)                     // default: min(available_parallelism(), 4)
    .tls(TlsServerConfig::from_pem(CERT, KEY)?)
    .http1(true)                     // defaults: h1 on, h2 on, h3 off
    .http2(true)
    .http3(true)                     // also binds UDP; without TLS, build fails with InvalidConfig
    .register_header::<XRequestId>() // extension type decoded on input; standard types built in
    .limits(Limits::default())
    .timeouts(Timeouts::default())
    .build()?;                       // binds TCP (and UDP when h3 is on); run() starts threads

let handle: ServerHandle = server.handle();   // Clone + Send + Sync
server.run(my_handler)?;                      // blocks until shutdown
```

- `ServerHandle`: `shutdown()`, `shutdown_with_deadline(Duration)`, `stats() -> Stats`,
  `local_addrs() -> Vec<SocketAddr>` (TCP), `local_udp_addrs() -> Vec<SocketAddr>`.
- `shutdown()` signals and returns. `Server::run` closes listeners, sends GOAWAY/`Connection: close`,
  gives in-flight exchanges `Timeouts::shutdown_grace` (default 30 s; `shutdown_with_deadline`
  overrides it for that call), cancels what remains, and joins the reactor threads, then returns
  `Ok(())`; `Err` only for a fatal listener/reactor failure. Normal handlers are drained
  within the grace period. A handler that never returns keeps its worker thread, so `run` does not
  return in that case; this is the one unbounded wait, caused by user code, and is documented rather
  than papered over. A handler that ignores cancellation but eventually returns does not prevent
  teardown.
- `run` consumes `Server`; the calling thread is neither reactor nor worker.
- `register_header::<T: Header>()` on the server (and client) builder adds an application-defined
  typed header to the input decoder registry. Standard headers are registered internally. A name
  collision with a standard or already-registered header is `InvalidConfig`. Registration is only
  needed to decode incoming fields; `set`/`append` of an outgoing typed header works without it.
- Port 0 with TCP+UDP: TCP binds first, UDP binds the assigned port; multiple addresses get separate
  pairs, reported by `local_addrs`/`local_udp_addrs`.
- `Handler` is shared across workers. Panics are caught: `500` if the head is not committed, otherwise
  reset/close; the worker continues. `panic = "abort"` profiles opt out.

### 6.3 Request, response, bodies

```rust
impl Request {
    pub fn method(&self) -> Method;
    pub fn target(&self) -> Target<'_>;
    pub fn version(&self) -> Version;
    pub fn headers(&self) -> &HeaderMap;
    pub fn header<T: Header>(&self) -> Option<&T>;              // typed
    pub fn headers_of<T: Header>(&self) -> impl Iterator<Item = &T>;
    pub fn header_value(&self, name: impl AsHeaderName) -> Option<HeaderRef<'_>>; // raw
    pub fn body(&mut self) -> &mut BodyReader;
    pub fn trailers(&self) -> Option<&HeaderMap>;   // after body EOF
    pub fn peer_addr(&self) -> Option<SocketAddr>;
    pub fn local_addr(&self) -> Option<SocketAddr>;
    pub fn is_secure(&self) -> bool;
    pub fn is_canceled(&self) -> bool;
}

impl Response {
    pub fn new() -> Self;                           // 200
    pub fn status(&self) -> StatusCode;
    pub fn set_status(&mut self, s: StatusCode) -> &mut Self;
    pub fn headers(&self) -> &HeaderMap;
    pub fn set<T: Header>(&mut self, value: T) -> &mut Self;       // typed, replaces
    pub fn append<T: Header>(&mut self, value: T) -> Result<&mut Self, HeaderError>; // rejects dupes unless MULTIPLE
    pub fn remove<T: Header>(&mut self) -> &mut Self;
    pub fn header(&mut self, name: impl IntoHeaderName, value: impl IntoHeaderValue)
        -> Result<&mut Self, Error>;                // raw/custom; validates; never panics
    pub fn remove_header(&mut self, name: impl AsHeaderName) -> &mut Self;
    pub fn body(&mut self) -> &mut BodyWriter;
    pub fn set_body(&mut self, body: impl IntoResponseBody) -> &mut Self;
    pub fn is_canceled(&self) -> bool;
}

impl BodyReader {
    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error>;   // 0 = EOF
    pub fn next_chunk(&mut self) -> Result<Option<BodyChunk>, Error>; // zero-copy view
    pub fn read_to_end(&mut self, limit: usize) -> Result<Vec<u8>, Error>;
    pub fn is_end(&self) -> bool;
}

impl BodyWriter {
    pub fn write(&mut self, buf: &[u8]) -> Result<usize, Error>;
    pub fn write_all(&mut self, buf: &[u8]) -> Result<(), Error>;
    pub fn flush(&mut self) -> Result<(), Error>;
    pub fn finish(&mut self) -> Result<(), Error>;
    pub fn finish_with_trailers(&mut self, trailers: HeaderMap) -> Result<(), Error>;
}

impl BodyChunk { pub fn as_bytes(&self) -> &[u8]; }   // Send + Sync, refcounted

impl Request {
    /// Atomic opt-in to the deferred path: moves the request head/body and the
    /// uncommitted response into Send handles. On error nothing is moved.
    pub fn defer(&mut self, res: &mut Response)
        -> Result<(DeferredRequest, DeferredResponse), Error>;
}

impl DeferredRequest {
    pub fn method(&self) -> Method;
    pub fn target(&self) -> Target<'_>;
    pub fn version(&self) -> Version;
    pub fn headers(&self) -> &HeaderMap;
    pub fn header<T: Header>(&self) -> Option<&T>;
    pub fn headers_of<T: Header>(&self) -> impl Iterator<Item = &T>;
    pub fn header_value(&self, name: impl AsHeaderName) -> Option<HeaderRef<'_>>;
    pub fn body(&mut self) -> &mut BodyReader;       // blocks this app thread
    pub fn trailers(&self) -> Option<&HeaderMap>;
    pub fn peer_addr(&self) -> Option<SocketAddr>;
    pub fn local_addr(&self) -> Option<SocketAddr>;
    pub fn is_secure(&self) -> bool;
    pub fn is_canceled(&self) -> bool;
}

impl DeferredResponse {
    pub fn status(&self) -> StatusCode;
    pub fn set_status(&mut self, s: StatusCode) -> &mut Self;
    pub fn headers(&self) -> &HeaderMap;
    pub fn set<T: Header>(&mut self, value: T) -> &mut Self;
    pub fn append<T: Header>(&mut self, value: T) -> Result<&mut Self, HeaderError>;
    pub fn remove<T: Header>(&mut self) -> &mut Self;
    pub fn header(&mut self, name: impl IntoHeaderName, value: impl IntoHeaderValue)
        -> Result<&mut Self, Error>;
    pub fn body(&mut self) -> &mut BodyWriter;       // blocks this app thread
    pub fn send(self) -> Result<(), Error>;          // commits/sends; consumes
    pub fn send_body(self, body: impl IntoResponseBody) -> Result<(), Error>;
    pub fn is_canceled(&self) -> bool;
}
```

`DeferredResponse::send` finishes an in-progress body writer and commits; `send_body` is shorthand,
and `BodyWriter::finish_with_trailers` is the trailer path. `DeferredRequest` and `DeferredResponse`
are `Send`, not `Sync`; they can be moved to one application thread (or app executor) and used
there. `Request::defer` is the only way to enter the
deferred path, and it moves both sides atomically:

- It fails with `Overloaded` when `max_deferred_exchanges` is reached, leaving both sides untouched;
  the handler can then take the normal synchronous path.
- It fails with `InvalidRequest` if the response head is already committed, a response body has
  already been set, or the request is already deferred; it moves nothing.
- On success the handler must return promptly; the borrowed `Request`/`Response` become tombstones
  whose methods return `Closed`/`InvalidRequest`. Success **transfers the exchange to the deferred
  handles**: the handler's later return value or panic only cancels the exchange and never commits
  the tombstoned response or synthesises a `500` for state it no longer owns. The library frees the
  worker, keeps the exchange in a bounded detached table, and wakes the reactor when the app thread
  writes or completes. The deferred path has the same capabilities as the synchronous one: typed and
  raw headers, streaming request and response bodies, trailers, deadlines, cancellation, h1
  keep-alive/ordering and h2/h3 stream multiplexing. It is not a reduced mode.
- If the handler returns `Err` or panics after deferring, the deferred exchange is cancelled (no
  tombstone response).
- Dropping `DeferredResponse` without `send` sends `500` if the head is uncommitted and the connection
  still permits it, otherwise resets/closes. Dropping `DeferredRequest` with an unread body abandons
  it, applies the normal drain/close/reset rule, and may cancel the deferred response.
- h1 parks that connection until completion (one deferred exchange per h1 connection; no next
  request); h2/h3 keep serving other streams and apply the normal per-stream flow control.
- `Timeouts::deferred` defaults to 60 s and is measured from the successful defer; `total_request`
  (when set) also applies and the earlier deadline wins. Expiry is `Timeout(Deferred)` and cancels
  the exchange; disconnect and shutdown are `Canceled`. Shutdown cancels deferred exchanges after
  grace.
- Capacity is a server-wide atomic reservation, not a per-reactor count: `Request::defer` reserves a
  detached slot first, checks the exchange terminal state, then publishes the handles into the owning
  reactor's table. A failed reservation or a terminal exchange returns `Overloaded`/`Canceled` with
  both sides untouched. The detached table is refcounted: expiry or shutdown unlinks the exchange and
  marks it terminal, but the `DeferredRequest`/`DeferredResponse` handles keep a reference to the
  terminal state, so a blocked application read/write wakes with `Canceled` and a later `send` also
  returns `Canceled`. Handles may outlive `Server::run`; they cannot touch freed rings or buffers.
- This is not automatic fallback: the library cannot take a handler that is already blocked inside a
  synchronous RPC call. The handler must call `defer` before it blocks.

Contract points settled here (details in execution/protocol documents):

- Reads and writes block the worker until data, capacity, a deadline, or cancellation; they never do
  socket I/O. Timeouts return `Timeout(kind)`, disconnect/shutdown/reset returns `Canceled`.
- `max_body_bytes` bounds the whole decoded request body for both streaming and buffered reads;
  crossing it makes the next read `BodyTooLarge`, sends `413` if the head is uncommitted, else resets
  or closes. Response bodies are handler-controlled and not bounded by it; the client has
  `max_response_body_bytes` (default `None`).
- `set_body` accepts `&'static [u8]`, `&'static str`, `String`, `Vec<u8>`, empty. Static bodies are
  written without a user copy. Streaming and `set_body` are mutually exclusive.
- Response head is committed at the first body write, at `finish`, or on handler return. There is no
  `headers_mut`: headers are set through `header` (raw) or `set`/`append` (typed), and library-owned
  framing fields (`Transfer-Encoding`, `Connection`) are rejected at commit with `InvalidRequest` for
  either path; the standard typed types for those fields exist for inspection. At commit the library
  also collapses/removes duplicate `Content-Length`, as a defence-in-depth check. A
  handler-set single `Content-Length` must match the body, otherwise reset/close. Trailers require
  chunked h1 framing; `Content-Length` + trailers is a handler error.
- `HEAD`/`1xx`/`204`/`304` bodies are discarded and no body framing is emitted.
- Trailer maps use the same `Header`/`HeaderMap` model: request trailers are decoded when the body
  reaches EOF, response trailers are encoded at `finish_with_trailers`. Trailer maps are checked at
  commit: framing fields (`Content-Length`, `Transfer-Encoding`, `Connection`) and `Host` are rejected
  as trailer fields, never emitted.
- Unread request body on return: HTTP/1.1 drains up to 64 KiB within 100 ms, then closes with
  `Connection: close`; HTTP/2 and HTTP/3 reset the stream immediately after the response and keep the
  connection.
- `Expect: 100-continue` is honored on first body read; other expectations get `417`.
- Invalid header names/values return `Error::InvalidRequest`; no panic on the setter path.

### 6.4 Typed model and header storage

```rust
pub enum Method { Get, Head, Post, Put, Delete, Connect, Options, Trace, Patch, Other(MethodExt) }
pub struct StatusCode(/* u16 */);
pub enum Version { Http10, Http11, Http2, Http3 }

pub struct HeaderName;      // known constants; custom names normalized, inline ≤32 bytes, else boxed
pub struct HeaderValue;     // owned/static/inline validated value; also the encoding output
pub struct HeaderRef<'a>;   // borrowed validated view over wire bytes; the decoding input
pub struct HeaderMap;       // ordered, duplicate-preserving, typed entries plus raw entries
pub struct HeaderError;     // decode/encode/registration error; stable Display, source chain

pub trait Header: Sized + Send + Sync + 'static {
    fn name() -> HeaderName;            // fn, not const: boxed custom names are allowed
    const MULTIPLE: bool = false;
    fn decode(value: HeaderRef<'_>) -> Result<Self, HeaderError>;
    fn encode(&self) -> Result<HeaderValue, HeaderError>;
}

pub enum Target<'a> { Origin{..}, Absolute{..}, Authority(&'a [u8]), Asterisk }
pub struct Uri;             // owned URI for clients
```

**One trait, all typed headers.** Standard and application headers implement the same `Header`
trait; there is no `ExtensionHeader` and no `HeaderMapExt`. `decode` takes one field line as a
`HeaderRef` (no `Iterator<&HeaderValue>`); `encode` returns one `HeaderValue` or a typed error (no
`Extend<HeaderValue>`); `MULTIPLE` says whether the field may repeat. `Header: Send + Sync` is
required: typed values live in `Send` maps and deferred handles. The name is a `fn` rather than a
`const` so a custom name longer than the inline capacity can still be boxed; standard names return
their static `HeaderName` without allocating, and registration caches the name. The map's inherent
methods are the access layer:

```rust
impl HeaderMap {
    pub fn get<T: Header>(&self) -> Option<&T>;
    pub fn get_all<T: Header>(&self) -> impl Iterator<Item = &T>;
    pub fn set<T: Header>(&mut self, value: T);
    pub fn append<T: Header>(&mut self, value: T) -> Result<(), HeaderError>;
    pub fn remove<T: Header>(&mut self);
    pub fn get_raw(&self, name: impl AsHeaderName) -> Option<HeaderRef<'_>>;
    pub fn append_raw(&mut self, name: impl IntoHeaderName, value: impl IntoHeaderValue)
        -> Result<(), HeaderError>;
    pub fn iter_raw(&self) -> impl Iterator<Item = (&HeaderName, HeaderRef<'_>)>;
}
```

**Storage.** `HeaderMap` is an ordered list of entries. A typed entry is
`(HeaderName, TypeId, arena handle)`; its value lives once in the head's typed arena. A raw entry is a
name plus a span into the head block owned by the map, or an owned `HeaderValue` for outgoing raw
fields; `get_raw`/`iter_raw` reconstruct a `HeaderRef<'_>` per call. There is no stored `HeaderRef`
borrowed from the same struct, no enum whose size is the largest header type, and users never match
storage variants. Typed access is `get::<T>()` or the generated inherent accessor
(`req.content_length()`, `res.set_content_type(...)`); raw access is `get_raw`/`iter_raw` and covers
raw entries only. `Header::decode` is called once per field line when the exchange is handed to the
handler/caller (queued exchanges hold raw heads only); `MULTIPLE` is enforced on input and on
`append`, so a duplicate singleton is `400`/stream reset. An unregistered extension field stays raw;
`get::<T>()` returns `None` until `register_header::<T>()` is called. For h2/h3,
`Request::header::<Host>()` synthesises `Host` from `:authority` when the field is absent; the raw
map contains a `Host` entry only if the peer sent one.

The typed arena's type-erased entries need drop and encode function pointers; those are invoked once
per typed header, never per byte or per lookup. Standard headers use generated accessors and decoder
entries; extension headers use the same trait through the registry. This is the honest vtable cost of
keeping the standard path free of an enum; `iter_raw` does not encode typed headers on demand.

**Fast path and extension.** The standard header types are generated with inherent accessors and a
known-name decoder table, so lookup and typed access avoid the registry and the type-id search.
Decode, encode and drop still cross one type-erased function pointer per typed field (the vtable cost
stated above); this is once per header, never per byte or per lookup. Application types are
registered on the builder (`register_header::<T>()`) and decoded through the same trait; their input
path uses the registry and a type-id lookup, while outgoing values use the same encode pointer.
Outgoing
`set`/`append` works for any `T: Header` without registration. Standard v1 coverage is the structured
RFC 9110/9111/9112 field set used by servers and clients: `Host`, `Content-Length`, `Content-Type`,
`Content-Encoding`, `Content-Language`, `Content-Location`, `Content-Range`, `Date`, `Age`, `Expires`,
`Cache-Control`, `Pragma`, `Vary`, `ETag`, `Last-Modified`, the `If-*` conditionals, `Range`,
`Accept-Ranges`, `Connection`, `Transfer-Encoding`, `TE`, `Trailer`, `Expect`, `Cookie`, `Set-Cookie`,
`Accept*`, `Authorization`, `Proxy-Authorization`, `WWW-Authenticate`, `Proxy-Authenticate`,
`Location`, `Referer`, `User-Agent`, `Server`, `Retry-After`, `Allow`, `Link`, and `Warning`. The
long tail and all tunnel-only fields stay raw.

**Cost.** Typed values are owned (`Self: 'static`), so variable-size values may allocate; the
per-head typed arena bounds them and `HeaderValue`'s inline storage absorbs short values. Raw custom
fields remain zero-copy spans over the head block. Typed-header allocations are measured and reported
rather than hidden; the zero-allocation claim applies to the raw path and to headers whose values fit
inline.

Arena bytes are not free-standing: the per-head arena is charged to `max_connection_buffer_bytes` and
to the server's `max_buffered_bytes`, exactly like body blocks. This covers active workers, detached
exchanges and trailer maps. If the reservation would exceed the connection or global cap, typed
decode fails with `431`/a stream error before the handler runs, and `Request::defer` fails with
`Overloaded`; there is no arena allocation outside the caps. `max_header_arena_bytes` is a per-head
ceiling, not a global sum. Client-side response arenas are 64 KiB per in-flight response and are
bounded by the client pool/stream caps and its request queue.

`Date` is generated per reactor per second; `Server` is off by default and configurable.

### 6.5 Limits, timeouts, stats, errors

```rust
pub struct Limits {
    pub max_head_bytes: u32,                  // h1 raw head; 16 KiB
    pub max_header_count: u32,                // 100 fields (h1 and h2/h3)
    pub max_body_bytes: Option<u64>,          // whole request body; 8 MiB server default; None = unlimited
    pub max_connections: usize,               // 100_000
    pub max_connections_per_reactor: usize,   // 50_000
    pub max_pending_accepts: usize,           // 1_024 acceptor -> reactor registrations
    pub max_inflight_exchanges: Option<usize>,// None = workers
    pub max_queued_exchanges: Option<usize>,  // None = workers * 256
    pub max_inflight_exchanges_per_connection: Option<usize>, // None = max(1, workers / 2)
    pub max_pipelined_requests: u32,          // 8
    pub max_request_body_buffer: usize,       // 64 KiB per-exchange ceiling
    pub max_response_body_buffer: usize,      // 64 KiB per-exchange ceiling
    pub max_trailer_bytes: u32,               // 8 KiB
    pub max_header_arena_bytes: u32,          // 64 KiB typed-header arena per head
    pub max_chunk_extension_bytes: u32,       // 1 KiB (h1)
    pub max_connection_buffer_bytes: usize,   // 8 MiB h2/h3, 256 KiB h1; effective total per stream = min(128 KiB, cap/open)
    pub max_buffered_bytes: usize,            // 256 MiB per server; low-water reset, not a global stall
    pub max_parked_workers: Option<usize>,    // None = max(1, workers / 2)
    pub max_parked_per_connection: Option<usize>, // None = max(1, parked / 2)
    pub max_parked_writers_per_connection: usize, // 1, within the per-connection total
    pub max_parked_readers_per_connection: usize, // 1, within the per-connection total
    pub max_deferred_exchanges: usize,        // 4,096 detached exchanges per server
    pub h2_max_concurrent_streams: u32,       // 32
    pub h2_initial_stream_window: u32,        // 64 KiB
    pub h2_initial_connection_window: u32,    // 1 MiB
    pub h2_max_header_list_bytes: u32,        // 64 KiB including 32 bytes/field
    pub h2_max_continuation_frames: u32,      // 8
    pub h2_header_table_size: u32,            // 4 KiB advertised/decoder table size
    pub h3_max_concurrent_streams: u32,       // 32
    pub h3_max_header_list_bytes: u32,        // 64 KiB including 32 bytes/field
    pub max_qpack_decoded_bytes: u32,         // 64 KiB cumulative, 32 bytes/field included
}

pub struct Timeouts {
    pub header_read: Duration,          // 10 s
    pub body_idle: Duration,            // 30 s (no bytes)
    pub read_stall: Duration,           // 15 s per blocking read
    pub write_stall: Duration,          // 15 s per blocking write
    pub keep_alive: Duration,           // 60 s
    pub tls_handshake: Duration,        // 10 s
    pub connect: Duration,              // 10 s (client)
    pub total_request: Option<Duration>,// off; applies to server and client
    pub deferred: Option<Duration>,     // 60 s; bounds a detached exchange after defer; None disables
    pub shutdown_grace: Duration,       // 30 s; overridden by shutdown_with_deadline
}
```

`None` values in `Limits` (`max_inflight_exchanges`, `max_queued_exchanges`,
`max_inflight_exchanges_per_connection`, `max_parked_workers`, `max_parked_per_connection`) are resolved from the builder's worker count at `build`; invalid
combinations are rejected there with `InvalidConfig`.

`Error` is the closed enum from the runtime/API sections:

```rust
pub enum Error {
    Io(std::io::Error), Timeout(TimeoutKind), Protocol(ProtocolError), Tls(TlsError),
    BodyTooLarge { limit: u64 }, HeadTooLarge { limit: u32 }, Overloaded, Canceled, Closed,
    InvalidRequest(&'static str),       // header/API misuse
    InvalidConfig(&'static str),        // builder or limit combination rejected at build
    Header(HeaderError),                // typed header decode/registration failure
}
pub enum TimeoutKind {
    Connect, TlsHandshake, HeaderRead, BodyIdle, ReadStall, WriteStall, KeepAlive, PoolQueue,
    Total, Deferred, Wait,
}
pub struct ProtocolError;   // version() + kind(); stable classification, not a wire dump
pub enum ProtocolErrorKind { MalformedMessage, Framing, HeaderCompression, FlowControl,
                             StreamState, Settings, Trailer, Unsupported }
pub struct TlsError;        // Display + source()
```

`ProtocolError` and `TlsError` are typed and testable through their kinds; `HeaderError` is the
typed decode/encode/registration error and maps a malformed known header to `400`/a stream error and
an invalid outgoing value to `500`/reset. Application `decode`/`encode` implementations run at the
library boundaries under `catch_unwind`: a panic in either is a `400` on input and a `500`/reset on
output, never a dead worker or reactor. On a deferred application thread the library cannot catch an
application `encode` panic; that thread unwinds, drops its handles, and the deferred drop rule applies
(`500` if possible, otherwise reset/close). No `Box<dyn Error>` on the request path. `Overloaded` means the client call queue is full, or no parking/write capacity is
available under the worker or per-connection caps. `on_error(Arc<dyn Fn(&Error) + Send + Sync>)` on the server/client builders observes
reactor-level errors (protocol, timeout, reset); handler return values are the handler's own to log.
The hook runs on the observing thread and must not block or call `shutdown`/`shutdown_with_deadline`
(it would self-join the reactor). `Stats` is a cheap relaxed-atomic snapshot
(connections, requests, failures, timeouts, resets, overflows, detached exchanges, queue depths,
bytes); there is no
public per-operation cost API.

### 6.6 Client (summary)

`Client` is `Clone + Send + Sync` and owns its reactors, resolver threads and pool. It has two
surfaces: `start`/`start_streaming` return a `ClientCall` without blocking on DNS, connect, pool
capacity or stream capacity; `send`/`send_with` are the blocking convenience wrappers
(`start(...).wait()`). `ClientCall` supports `wait`, `try_wait`, `wait_timeout`, `cancel_handle`,
`body` for streaming upload, and `on_complete(Arc<dyn Executor>, callback)`; the executor is supplied by
the application and is the only place a completion callback runs. There is no async runtime, no
`Future`/`Waker`, and no library-owned executor. `ClientHandle` provides `shutdown`,
`shutdown_with_deadline` and `stats`; calls hold the client alive, and the last handle joins the
reactors (resolver threads detach at shutdown).

Protocol selection is explicit: `http1`, `http2`, `h2c` (prior knowledge), `http3` (opt-in, no
Alt-Svc/fallback). Pool key is `(scheme, host, port, ALPN)`; no certificate coalescing. h1 opens up
to 8 connections per host, h2/h3 up to 2, then queues; the pool is capped at 1,024 connections
and 256 idle globally, in-flight calls at 4,096, buffered queued-body bytes at 64 MiB, and the call
queue at 1,024. A 9/9 review pass fixed the Arc executor, DNS-cancel, refcounted-handle, atomic
completion, resolver-mailbox and global-bound issues.
`start` returns `Overloaded` only when the queue is full; a queued call times out with
`Timeout(PoolQueue)`. DNS runs on bounded resolver threads with a 60 s cache and Happy Eyeballs
(250 ms stagger), never on the caller or a reactor. One retry only for a reused connection, before
response bytes, idempotent method, replayable body. Requests and responses use typed headers through
the same `Header` trait (`ClientRequest::set(ContentType::json())`,
`ClientResponse::headers().get::<ContentType>()`), with raw `header`/`header_value` for custom fields
and `ClientBuilder::register_header::<T>()` for incoming extension types. Header limits are the fixed
protocol defaults, not configurable in v1. No redirects, cookies, compression, or CONNECT. Full
contract in the [client design](2026-10-26-libhttp-client-design.md).

### 6.7 TLS configuration

`TlsServerConfig`/`TlsClientConfig` wrap rustls: `from_pem`, ALPN, client auth, versions; client
roots default to `webpki-roots`. rustls types are not part of the public API in v1 (the backend is
kept provisional behind G1-TLS; a custom verifier/resumption bridge is phase 2). v1 uses one
certificate configuration per
listener (no SNI switching), no 0-RTT, no session-ticket rotation policy. `close_notify` is sent on
graceful close and required on shutdown; truncation is an error. A C toolchain is required for the
default build because `ring` is native; this is accepted because QUIC uses the same provider and no
production pure-Rust TLS provider exists. `aws-lc-rs` is not offered in v1.

---

## 7. The twelve §10 decisions

Every §10 question is settled. The contracts behind each decision are in the companion documents.

| # | Question | Decision |
|---|---|---|
| 1 | I/O model across platforms | No public I/O model. Engines are sans-I/O byte-flow machines; each platform uses its native primitive (epoll/kqueue readiness, IOCP completion). io_uring later is another transport. → runtime §2 |
| 2 | Read/accept arming, backpressure, close | One logical read and one write in flight; pause by clearing interest/not reposting; terminal read = EOF/error; op identity = `(slot, generation)`; close cancels/wait for completions; accept loops on readiness and a bounded `AcceptEx` pool on Windows. → runtime §2.3, §6 |
| 3 | Buffer ownership | 16 KiB refcounted blocks from a bounded per-reactor pool; ranges, not copies; one atomic per block crossing threads; batched returns with one coalesced wake; explicit head storage, quotas and copy/atomic budget. → runtime §4 |
| 4 | Worker model | One shared, bounded pool; handlers only on workers; single-owner reactor deadlines; `read_stall`/`write_stall`; global parked-worker ceiling plus per-connection reader/writer caps; per-stream connection-window shares; explicit admission and cancellation; per-request deferred completion on the same handler trait; no
preemption of blocking handlers. → execution §6, §9 |
| 5 | Fast-path scope | v1 native baselines; optional gated `SO_REUSEPORT`/`TCP_FASTOPEN`/`EPOLLEXCLUSIVE`/Linux datagram batching; all zero-copy and io_uring paths phase 2 behind the same seam. → runtime §9 |
| 6 | QUIC backend and budget | `quinn-proto` chosen on merit (sans-I/O, rustls, no async), not obligation; h3 framing + static QPACK ours; datagram copy/syscall budget stated; quiche remains the measured fallback if G3 fails. → protocols §4 |
| 7 | TLS integration and order | rustls, one `ring` provider, ALPN, `close_notify`, nonblocking handshakes; TLS lands after HTTP/1.1 and before HTTP/2; TLS performance gate G1-TLS. → runtime §3, §7 |
| 8 | File-bodied responses | No distinct file-body API in v1; handlers stream files through `BodyWriter`. Deferred because opening/reading on a reactor blocks and sendfile breaks under TLS. → §2.2 |
| 9 | Half-close and tunnels | Internal only (TLS `close_notify` + `SHUT_WR`/`SD_SEND`, h1 early responses); no public CONNECT/`101`/tunnel. → runtime §6 |
| 10 | Socket ownership | Reactor-owned slot table with generations; single acceptor + handoff; Windows IOCP association before first use; close/drain order; QUIC UDP reactor limits. → runtime §2.5, §6 |
| 11 | Performance gate | G1 (h1), G1-TLS, G2 (h2), G3 (h3) on a fixed host with a fixed core budget; syscalls include futex/eventfd; 10k idle connections measured; a miss blocks the milestone and never relaxes correctness. → §8 |
| 12 | Crate layout, features, MSRV, deps | One published crate; explicit feature set with `http3` requiring `tls`; `ring` only; MSRV 1.85; dependencies: rustls, rustls-pki-types, rustls-pemfile, quinn-proto, webpki-roots, ring (transitive), libc, windows-sys. → §10 |

---

## 8. Performance gates

The harness is a deliverable. Gates are attached to milestones that can run the real endpoint.
Every number is published with the host, kernel, rustc, hyper/tokio/rustls/quinn versions.

| Gate | Milestone | Workload | Pass condition |
|---|---|---|---|
| G1 | M2 (h1 plaintext) | keep-alive, 15-byte static body, 64/256/1024 conns, `wrk`, no pipelining | throughput ≥ 0.95× hyper 1.12 + tokio 1.54; p99 ≤ 1.25×; syscalls/request ≤ 1.1×; steady-state allocs/request ≤ 1 for the benchmark header set; typed-header allocations counted separately |
| G1-TLS | M3 (h1 TLS) | same, TLS on, same rustls config | throughput ≥ 0.90× hyper + rustls; p99 ≤ 1.25×; TLS overhead vs our plaintext published |
| G2 | M4 (h2 plaintext) | `h2load`, 1/10/32 streams, keep-alive | throughput ≥ 0.95× hyper h2; p99 ≤ 1.25× |
| G3 | M5 (h3) | `h2load --h3`/`nghttp3` client, 1/10/32 streams | ≥ 0.95× the quiche 0.30 h3 server (pinned Docker image) on the same host; syscalls/datagram ≤ 1.1× the batched-path estimate |

- **Core budget:** the server under test gets the same number of physical cores as the hyper/tokio
  configuration, pinned and isolated; our `reactors + workers` sum to the core count (e.g., 2 + 6
  against an 8-worker tokio runtime). If our split is not competitive, the gate is re-measured with
  the best split and that split is published. Load generator on separate pinned cores.
- **Syscalls** include `read`/`write`/`sendmsg`/`recvmsg` and futex/eventfd/IOCP wakes, counted by
  the harness, not just network calls.
- **Also measured:** 10k idle plaintext connections per reactor (library-side RSS target ≤1 KiB/conn;
  TLS measured separately) and TLS overhead.
- **On a miss:** the milestone is not complete, no optional fast path becomes a default, and the hot
  path is profiled and fixed. Correctness, limits, and timeouts are never relaxed. If the target is
  structurally unreachable after profiling, implementation stops and the measured evidence plus
  options go to the project owner as an explicit scope decision; a slower number is published only if
  the owner accepts it.

---

## 9. Portability and CI

| Capability | Linux | macOS | FreeBSD | Windows |
|---|---|---|---|---|
| TCP readiness | epoll (ET) | kqueue (`EV_CLEAR`) | kqueue (`EV_CLEAR`) | — |
| TCP completion | io_uring phase 2 | — | — | IOCP |
| Reactor wakeup | eventfd | pipe | pipe/kqueue | `PostQueuedCompletionStatus` |
| Accept | acceptor + optional `SO_REUSEPORT` | acceptor (no LB) | acceptor + `SO_REUSEPORT_LB` phase 2 | acceptor + IOCP handoff |
| UDP batching | `recvmmsg`/`sendmmsg`/GSO | single datagram | `recvmmsg` | single datagram; USO phase 2 |
| QUIC reactors | one per reactor/address | one reactor owns all UDP sockets | one per reactor/address | one reactor owns all UDP sockets |
| Half-close | `SHUT_WR` | `SHUT_WR` | `SHUT_WR` | `SD_SEND` |
| `SIGPIPE` | `MSG_NOSIGNAL` | `SO_NOSIGPIPE` | `SO_NOSIGPIPE` | n/a |

CI builds and runs functional tests on Linux, macOS, Windows and FreeBSD for every milestone; all
four are release gates. macOS covers the kqueue path without `recvmmsg`/load-balancing reuseport;
FreeBSD runs in a VM (`vmactions/freebsd-vm` or equivalent) and covers the kqueue path with
`recvmmsg`. If the FreeBSD runner is unavailable, the release is not cut rather than declaring the
BSD platform untested. Runtime probes decide optional accelerators; absence degrades performance only.
No public type or behavior differs by platform. Numbers are published per platform, never
extrapolated.

---

## 10. Packaging, features, MSRV, dependencies

- **One published crate** `libhttp`; workspace members `bench/` and `fuzz/` are unpublished and never
  dependencies of it. Public modules as in §6.1; everything else private.
- **Features:** `server`, `client`, `http1`, `http2`, `http3`, `tls`. Default: all six. `http3`
  requires `tls`, and at least one of `server`/`client` and one of `http1`/`http2`/`http3` is
  required; enforced with `compile_error!`. `h2c` requires `http2` (rejected at build with
  `InvalidConfig`). There is no provider feature in v1 (only `ring`), so
  `cargo build --all-features` has no ambiguity and a no-TLS build drops rustls and QUIC entirely.
  The CI feature matrix includes server-only h1, client-only h2, no-TLS, h3, `--all-features`, and a
  build with every protocol disabled (must fail at compile time).
- **MSRV:** 1.85.0 (`rust-version = "1.85"`), edition 2024, resolver v3, tested with the actual 1.85
  toolchain on all CI platforms (Linux, macOS, Windows; FreeBSD VM). Raising MSRV requires a minor release and a note; the lockfile is
  MSRV-checked.
- **Unsafe policy:** unsafe code is confined to the per-platform syscall/FFI modules and the buffer
  refcount internals; each block is documented and wrapped in a safe abstraction. No public API
  requires `unsafe`, and the crate builds with `#![forbid(unsafe_op_in_unsafe_fn)]`. Miri runs the
  safe test suite where the platform permits; the syscall paths are covered by integration tests on
  real sockets instead.
- **Dependencies:** `rustls` (0.23.x; `default-features = false`, `std`, `tls12`, `ring`),
  `rustls-pki-types`, `quinn-proto` (`default-features = false`, `rustls-ring`, `bloom`),
  `webpki-roots`, `rustls-pemfile` (public `from_pem`), `libc` (Unix), `windows-sys` (Windows;
  pinned at implementation time), `ring` transitively. Dev/bench only: `hyper` 1.12, `hyper-util`,
  `tokio` 1.54, `h2`, `proptest`, `libfuzzer-sys`. No `bytes` in the public API, no `socket2`/`mio`,
  no async runtime in the published graph.

---

## 11. Milestones

**M0 — Skeleton.** Crate layout, public types/builders, error enums, CI matrix, MSRV job, design
acceptance.

**M1 — Transport spine.** epoll/kqueue/IOCP; reactor loop; timer heap; block pool; accept/connect/
read/write/close; byte-echo server on the transport. Tests: loopback echo, 10k idle connection memory,
close/cancel, no blocking socket calls on reactors, Windows handoff. Exit: echo works on all CI platforms (Linux, macOS, Windows; FreeBSD VM); idle memory measured.

**M2

**M2 — HTTP/1.1 server and client, workers.** Parser/serializer, streaming bodies, dispatch, queues,
deadlines, cancellation, keep-alive, pipelining, h1c, smuggling corpus, fuzz targets. Exit: **G1**
passes; deferred completion works with streaming bodies, typed headers, trailers and h1/h2 ordering;
client interoperates with external servers.

**M3 — TLS.** rustls, ALPN, `close_notify`, client roots/certs, handshake timeouts, TLS copy/syscall
numbers. Exit: h1 over TLS, truncation tests, **G1-TLS** passes.

**M4 — HTTP/2.** HPACK, frames, stream states, flow control shares, per-stream dispatch,
GOAWAY/RST, trailers, h2c and ALPN. Exit: h2spec, interop, **G2**, slow-stream tests show no
connection stall.

**M5 — HTTP/3.** quinn-proto transport, h3 framing, static QPACK, control streams, server/client,
Linux datagram batching. Exit: interop against curl/quiche, QPACK size-budget tests, **G3** met; if
not, the measured gap and options go to the project owner as a scope decision.

**M6 — Hardening and release.** Long fuzz runs, soak tests, per-platform benchmark publication, API
review/docs, examples. Exit: docs/examples/harness run from a clean checkout; no known correctness or
unbounded-resource issues.

---

## 12. Risks

- **Blocking handlers cannot be preempted.** A handler that blocks before deferring holds a worker
  until it returns; the pool is protected from I/O parking, not from user threads. The deferred path
  is an explicit opt-in before blocking, not a preemption mechanism.
- **Hidden low-level API closes doors.** Advanced users cannot build a proxy or custom transport on
  this crate in v1. Accepted by the owner; proxy can return as a higher-level feature later.
- **QUIC scope.** h3 + QPACK is the largest risk. Mitigated by quinn-proto owning transport,
  static-table-only QPACK, last-milestone placement, interop tests, and a measured quiche fallback.
- **Windows IOCP.** Cancellation, `AcceptEx` reposting and buffer lifetime are the easiest
  use-after-free. Mitigated by landing IOCP in M1 with slots held until all completions arrive and
  generation checks.
- **Performance vs hyper.** The sync handoff costs a queue and wakeups per request; G1 includes them
  in the syscall budget. If G1 misses, dispatch is profiled before any optional fast path ships.
- **Native crypto build dependency.** `ring` needs a C toolchain; accepted because QUIC shares it.
- **Single QUIC reactor on macOS/Windows.** h3 throughput there is capped by one UDP reactor; stated,
  not an API difference.
- **rustls/latency.** rustls is kept only if G1-TLS passes; otherwise the cause is profiled before
  any scope change.

---

## 13. What is left to the builder

- Module/file structure, private type and trait names, parser table layout, error-message text.
- Representation of blocks, slabs, timer heaps and return queues, within the runtime contracts.
- The internal reactor/transport/TLS/engine API, provided engines stay sans-I/O.
- Benchmark harness code, load-generator configuration, CI YAML, fuzzer corpora, examples, docs.
- Tuning buffer sizes and queue depths within the published bounds, provided any public limit or
  documented budget change is called out in the design.
- Pinned versions for `windows-sys`, `libc` and tooling crates, subject to §10.
- The generated standard header catalogue, its accessor names, and the typed-arena/type-id mechanics,
  within the `Header` trait, storage and cost rules above.
- The detached-exchange table layout and completion wakeup mechanics, within the deferred contracts
  above.

**Not** left to the builder: public API semantics (§6), arming/pause/close contracts (runtime §2.3,
§6), buffer and copy/atomic/syscall budgets (runtime §4), worker/parking/deadline/cancellation
semantics (execution §4–§7), per-stream isolation (execution §6), socket ownership (runtime §6),
protocol strictness and framing (protocols), QPACK/QUIC scope (§7 table row 6, protocols §4), dependency/MSRV policy (§10),
and the performance gates (§8).

---

## 14. Verified facts and references

- quinn-proto 0.11.19: `Endpoint::handle(now, remote, local_ip, ecn, BytesMut, &mut Vec<u8>)`;
  `Connection::poll_transmit(now, max_datagrams, &mut Vec<u8>) -> Option<Transmit>` with
  `segment_size`; `Connection::poll_timeout()`; MSRV 1.85; default `rustls-ring`; QUIC TLS configs via
  `quinn_proto::crypto::rustls`.
  <https://docs.rs/quinn-proto/0.11.19/quinn_proto/>, <https://github.com/quinn-rs/quinn>.
- rustls 0.23.46, MSRV 1.71; sans-I/O `read_tls`/`write_tls`/`process_new_packets`; `IoState::peer_has_closed`;
  default `aws-lc-rs`, `ring` optional (we select `ring` and disable defaults).
  <https://docs.rs/rustls/0.23.46/rustls/>, <https://github.com/rustls/rustls>.
- QPACK static-table-only is conformant (RFC 9204 §2.1, §3.2.3); curl/quic-go/quiche/picoquic/msquic
  advertise zero capacity: <https://www.rfc-editor.org/rfc/rfc9204.html>,
  <https://pqcrypta.com/conformance/matrix/>. QPACK decoded-size amplification:
  <https://foxio.io/blog/header-accordions-qpack-amplification-attacks-against-http-3-servers>.
- hyper 1.12 / tokio 1.54: <https://github.com/hyperium/hyper/blob/master/CHANGELOG.md>,
  <https://crates.io/crates/tokio>. Edition 2024 stabilised in 1.85:
  <https://blog.rust-lang.org/2025/02/20/Rust-1.85.0/>.
- epoll ET/`epoll_pwait2`: <https://man7.org/linux/man-pages/man7/epoll.7.html>,
  <https://man7.org/linux/man-pages/man2/epoll_wait.2.html>.
- kqueue, macOS `SO_REUSEPORT`/no `recvmmsg`:
  <https://man.freebsd.org/cgi/man.cgi?query=kqueue&sektion=2>,
  <https://man.freebsd.org/cgi/man.cgi?query=recv&sektion=2>.
- IOCP/`AcceptEx`/`CancelIoEx`/`GetQueuedCompletionStatusEx`, no `SO_REUSEPORT`:
  <https://learn.microsoft.com/en-us/windows/win32/api/winsock2/nf-winsock2-wsarecv>,
  <https://learn.microsoft.com/en-us/windows/win32/api/mswsock/nf-mswsock-acceptex>,
  <https://learn.microsoft.com/en-us/windows/win32/fileio/cancelioex-func>.
- io_uring multishot/provided buffers/`SEND_ZC`/cancellation/overflow, seccomp and ABI instability:
  <https://man7.org/linux/man-pages/man3/io_uring_prep_recv_multishot.3.html>,
  <https://man7.org/linux/man-pages/man7/io_uring_provided_buffers.7.html>,
  <https://man7.org/linux/man-pages/man3/io_uring_prep_send_zc.3.html>,
  <https://man7.org/linux/man-pages/man7/io_uring_cancelation.7.html>.
- `recvmmsg` timeout bug, `MSG_ZEROCOPY`, `sendfile`:
  <https://man7.org/linux/man-pages/man2/recvmmsg.2.html>,
  <https://docs.kernel.org/networking/msg_zerocopy.html>.

---

## 15. Change log

- **2026-10-26:** first clean-room design. Supersedes the public low-level API, proxy scope (including
  h1 CONNECT/`101`/half-close), and the "proxy is the acid test" decision of the requirements brief;
  records the streaming body decision, static-QPACK v1, `ring`-only provider, single-acceptor portable
  listener model, and deferral of file bodies, tunnels, io_uring and dynamic QPACK. No prior design
  was read.
- **2026-10-26 (builder check):** answered eight questions in place: client pool blocking/timeout and
  `TimeoutKind::PoolQueue`; `max_body_bytes` scope and mid-stream behavior; client protocol-selection
  methods and request body/timeout setters; shutdown/`run` semantics; `max_pipelined_requests`; a
  fixed G3 reference (quiche 0.30 in Docker); the fifth parked writer's reset/`Overloaded` reaction;
  and concrete `ProtocolError`/`TlsError`/`on_error` surfaces.
- **2026-10-26 (adversarial review):** split the single document into this top-level design plus
  runtime, execution, protocol and client designs. Fixed all eleven findings: single-owner deadlines
  (workers wait untimed); global parked-worker ceiling plus read-side caps and `read_stall`;
  `max_connection_buffer_bytes`/`max_buffered_bytes` with idle/active memory arithmetic; per-stream
  connection-window shares making the h2 anti-stall rule reachable; concrete head storage and
  lifetimes; fallible outbound header validation; reconciled h1/h2/h3 head budgets; a buildable
  feature matrix (`http3` requires `tls`, `ring` only, `--all-features`); `BodyChunk` ownership and
  post-shutdown behavior; syscalls including futex/eventfd; complete supersede scope. Added the
  G1-TLS gate and a QUIC-backend evaluation that treats quinn-proto as a choice, not a mandate.
- **2026-10-26 (fresh adversarial review):** added the exchange sequence to timer keys; raised the
  per-connection budget to 8 MiB, reduced default h2/h3 streams to 32 and made per-stream buffers
  effective shares; added `max_inflight_exchanges_per_connection` and a per-connection parking share;
  removed `Response::headers_mut` and added commit-time framing filtering; made worker-derived
  `Limits` fields optional; added client `Timeouts`, `shutdown_grace`, `InvalidConfig`, missing
  bounds (chunk extensions, accept queue, block-pool cap, header overhead); removed rustls types from
  the public API; separated `HeaderValue` (owned) from `HeaderRef` (view); extended the supersede
  scope; rewrote h3 accounting as pull-limiting; scoped limit reactions by protocol; defined early
  final responses; replaced the global-cap stall with a low-water reset; made FreeBSD a CI gate; and
  fixed the remaining cross-reference, feature-guard and dependency issues.

- **2026-10-26 (fresh review closure):** verified 12/12 typed-header/deferred fixes and 6/6
  cross-document consistency fixes. Hardened `Header` with `Send + Sync`; replaced stored
  `HeaderRef` with spans over a map-owned head block; admitted the per-field drop/encode vtable and
  narrowed `get_raw`/`iter_raw` to raw entries; charged typed-header arenas to the connection and
  global caps; made defer's success transfer ownership; specified the server-wide atomic reservation,
  `Canceled`, and refcounted post-`run` handles; unified deferred timeout as `Timeout(Deferred)` at
  60 s; counted detached exchanges as open; enforced `MULTIPLE` on input and synthesised h2/h3
  `Host`; replaced `const NAME` with `fn name()`; made `encode` fallible and caught codec panics;
  moved `Timeouts` to the crate root.
- **2026-10-26 (client rework):** replaced the blocking-only client with a two-surface design:
  `start`/`start_streaming` + `ClientCall` (`wait`, `try_wait`, `wait_timeout`, `on_complete`,
  `cancel_handle`, streaming `body`) plus `send`/`send_with` convenience wrappers. Added
  `ClientHandle` shutdown/stats, `CancelHandle`, an explicit application `Executor` for completion
  callbacks, `Timeout(Wait)`, bounded resolver threads with Happy Eyeballs, and configurable h1/h2/h3
  connection caps (8/2/2). The call queue is 1,024 with a 30 s timeout; no async runtime,
  `Future`/`Waker` or library-owned executor.
