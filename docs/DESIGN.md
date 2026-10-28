# libhttp v1.0 — Frozen Design

**Status:** frozen and locked 2026-10-28 after builder review. Approved scope: HTTP/1.1 server + client + TLS.
**Supersedes:** the archived 2026-10-26/27 design set on
`archive/pre-rewrite-2026-10-28`. That branch is reference only; this file is the contract.


> **Implementation note (2026-10-28):** the frozen public API is delivered. The v1.0 client
> uses blocking sockets on the caller/worker thread with one body-pump thread per in-flight
> response; the reactor-driven client path in §2/§3.4 is post-1.0. Every deviation is recorded
> in `STATUS.md`; the public signatures and semantics are unchanged.

This is deliberately short. It exists to stop design drift: if an implementation question is
not answered here, choose the simplest option that preserves the public API and record the
choice in `STATUS.md`; do not reopen the architecture.

---

## 1. What this is

`libhttp` is a synchronous Rust HTTP library, not a framework. A handler is an ordinary
function; the library runs it on a bounded worker pool. No async runtime, `Future`, `Waker`,
executor or thread-per-connection exists anywhere in the product. Reactor threads own sockets
and never run user code; workers run handlers and block only on bounded library queues or
their own I/O.

**v1.0 scope**

- HTTP/1.1 server and client: request/response parsing and serialization, chunked bodies,
  trailers, `100-continue`, keep-alive, pipelining (ordered, processed sequentially in v1),
  HTTP/1.0 behavior, strict framing and request-smuggling defenses.
- Streaming request and response bodies with blocking, deadline-aware `BodyReader`/
  `BodyWriter`; buffered `set_body`.
- Synchronous handlers, typed and raw headers, typed errors, timeouts, cancellation, graceful
  shutdown, deferred completion with terminal fallback.
- TLS for HTTP/1.1 via `rustls` (ring provider), ALPN `http/1.1`, `close_notify`, PEM server
  certs, PEM/webpki client roots, optional client certificates.
- Linux, macOS and Windows. FreeBSD builds are kept working where the toolchain is available,
  but is not a v1.0 release gate.
- A client connection pool, DNS on a resolver thread, blocking `send`/`send_with` and callback
  `fetch`/`fetch_with`.

**Explicitly not in v1.0**

HTTP/2, HTTP/3, QUIC, reverse proxy, public low-level I/O API, CONNECT/`101`/Upgrade/WebSocket,
HTTP/2 push/priority, dynamic QPACK, file-body API, io_uring, multishot, provided buffers,
zero-copy send, `sendfile`/`splice`, kTLS, automatic compression/cookies/redirects, routing,
middleware, extractors. `Version::Http2`/`Http3` exist as values for forward compatibility but
no builder or parser accepts them.

---

## 2. Architecture

```text
  application thread / worker                     reactor thread
  ┌──────────────────────────────┐   bounded     ┌──────────────────────────────┐
  │ Handler::handle              │   rings +     │ mio Poll (epoll/kqueue/IOCP) │
  │ BodyReader / BodyWriter      │◄─condvars────►│ socket slab + timer heap     │
  │ Deferred handles             │   waker       │ HTTP/1.1 parser/serializer   │
  └──────────────────────────────┘               │ TLS via rustls               │
                                                 └──────────────────────────────┘
```

- `Runtime` owns reactors, the worker pool, resolver thread(s), admission and lifecycle.
  `Server` and `Client` are front-ends; they add listeners/handlers or the client pool but own
  no threads.
- One `Runtime` can host a server and clients. `Runtime::server()`/`Runtime::client()` attach;
  `Server::builder()`/`Client::builder()` create a private runtime.
- Transport is `mio`: nonblocking sockets, readiness on Unix, IOCP-backed readiness on Windows.
  The public API exposes no platform type. `reactors(n)` starts n reactor threads; v1 accepts
  server connections on one acceptor reactor and hands accepted sockets to the other reactors
  where the platform permits, otherwise keeps them on the acceptor. Client connections are
  distributed round-robin.
- The HTTP/1.1 engine is sans-I/O: bytes in, events out, bytes out. It never touches a socket.
- Bodies move through bounded per-exchange rings. The reactor is the only reader/writer of the
  socket. A worker blocks on a ring/condvar; the reactor never blocks.
- One timer heap per reactor is the only deadline authority. Entries are generation- and
  exchange-sequence-checked, so a stale timer cannot cancel a newer exchange on a reused
  connection.
- Third-party types never appear in the public API. `bytes::Bytes` backs `BodyChunk`
  privately; `httparse` parses heads privately; `rustls` config is wrapped privately.

---

## 3. Public API

The crate is `libhttp`; modules `libhttp::server`, `libhttp::client`, `libhttp::tls`, and
`libhttp::header`. Everything listed here is public contract; private module layout is free.

### 3.1 Crate root

```rust
pub struct Runtime;                         // Clone + Send + Sync
impl Runtime {
    pub fn builder() -> RuntimeBuilder;
    pub fn server(&self) -> ServerBuilder;
    pub fn client(&self) -> ClientBuilder;
    pub fn handle(&self) -> RuntimeHandle;
}
pub struct RuntimeBuilder;
impl RuntimeBuilder {
    pub fn reactors(self, n: usize) -> Self;              // default min(parallelism, 4)
    pub fn workers(self, n: usize) -> Self;               // default parallelism
    pub fn resolver_threads(self, n: usize) -> Self;      // default 1
    pub fn max_client_callbacks(self, n: usize) -> Self;  // default max(1, workers / 2)
    pub fn build(self) -> Result<Runtime, Error>;
}
pub struct RuntimeHandle;                   // Clone + Send + Sync
impl RuntimeHandle {
    pub fn shutdown(&self);
    pub fn shutdown_with_deadline(&self, grace: Duration);
    pub fn wait(&self) -> Result<(), Error>;
    pub fn stats(&self) -> RuntimeStats;
}
```

`reactors(0)`/`resolver_threads(0)` are `InvalidConfig`. `workers(0)` is allowed for a
blocking-only client; `rt.server()` and `fetch`/`fetch_with` reject it with `InvalidConfig`.
Workers start lazily on the first server or callback call; resolvers on the first client.

Typed model and errors:

```rust
pub enum Method { Get, Head, Post, Put, Delete, Connect, Options, Trace, Patch, Other(MethodExt) }
pub struct StatusCode(u16);
pub enum Version { Http10, Http11, Http2, Http3 }
pub struct HeaderName; pub struct HeaderValue; pub struct HeaderRef<'a>;
pub struct HeaderMap; pub struct HeaderError;
pub trait Header: Sized + Send + Sync + 'static {
    fn name() -> HeaderName;
    const MULTIPLE: bool = false;
    fn decode(value: HeaderRef<'_>) -> Result<Self, HeaderError>;
    fn encode(&self) -> Result<HeaderValue, HeaderError>;
}
pub enum Target<'a> { Origin { .. }, Absolute { .. }, Authority(&'a [u8]), Asterisk }
pub struct Uri;                              // owned absolute URI for clients
pub struct Timeouts; pub struct CancelToken;
pub struct BodyReader; pub struct BodyWriter; pub struct BodyChunk;
pub enum Error { Io(std::io::Error), Timeout(TimeoutKind), Protocol(ProtocolError),
                 Tls(TlsError), BodyTooLarge { limit: u64 }, HeadTooLarge { limit: u32 },
                 Overloaded, Canceled, Closed, InvalidRequest(&'static str),
                 InvalidConfig(&'static str), Header(HeaderError) }
pub enum TimeoutKind { Connect, TlsHandshake, HeaderRead, BodyIdle, ReadStall, WriteStall,
                       PoolQueue, Total, Deferred }
pub struct ProtocolError; pub enum ProtocolErrorKind { MalformedMessage, Framing,
                 HeaderCompression, FlowControl, StreamState, Settings, Trailer, Unsupported }
pub struct TlsError;                         // Display + source()
```

`Method` is case-sensitive for known methods and validates custom tokens; `Method::GET` etc.
are associated constants. `StatusCode` accepts 100..=599. `Version::from_bytes` accepts only
`HTTP/1.0`/`HTTP/1.1` in v1. `HeaderName` is lowercase, token-validated, static/inline/boxed
storage. `HeaderValue` rejects CR, LF, NUL, DEL and other CTLs; obs-text is allowed.
`HeaderRef<'a>` is a borrowed validated view. `HeaderMap` is ordered, duplicate-preserving,
case-insensitive on lookup, and supports both typed and raw entries.

`HeaderMap` interface:

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

Raw access covers raw entries only; typed values are opaque and are encoded when a message is
serialized. `MULTIPLE` is enforced on input and on `append`. Standard typed headers live in
`libhttp::header` and are re-exported at the root. The v1 catalogue is: `Host`,
`ContentLength`, `ContentType`, `ContentEncoding`, `ContentLanguage`, `ContentLocation`,
`ContentRange`, `Date`, `Age`, `Expires`, `CacheControl`, `Pragma`, `Vary`, `ETag`,
`LastModified`, `IfMatch`, `IfNoneMatch`, `IfModifiedSince`, `IfUnmodifiedSince`, `IfRange`,
`Range`, `AcceptRanges`, `Connection`, `TransferEncoding`, `TE`, `Trailer`, `Expect`,
`Cookie`, `SetCookie`, `Accept`, `AcceptEncoding`, `AcceptLanguage`, `Authorization`,
`ProxyAuthorization`, `WwwAuthenticate`, `ProxyAuthenticate`, `Location`, `Referer`,
`UserAgent`, `Server`, `RetryAfter`, `Allow`, `Link`, `Warning`. The long tail is raw.

`Uri` accepts absolute `http`/`https` URIs (scheme, authority, host, port, path, query) and
rejects relative/userinfo forms in v1. `Target` parses origin-form, absolute-form,
authority-form and asterisk-form; method-specific restrictions are enforced by the server.

### 3.2 Bodies

```rust
impl BodyReader {
    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error>;   // 0 = EOF
    pub fn next_chunk(&mut self) -> Result<Option<BodyChunk>, Error>;
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
impl BodyChunk { pub fn as_bytes(&self) -> &[u8]; }
```

Reads/writes block the worker/caller until data, capacity, a deadline, or cancellation. They
never perform socket I/O. `max_body_bytes` bounds the whole decoded request body; crossing it
yields `BodyTooLarge`, sends `413` if the head is uncommitted, else closes. `BodyChunk` is
`Send + Sync`, immutable and refcounted; retaining one charges the pinned quota. `set_body`
accepts `&'static [u8]`, `&'static str`, `String`, `Vec<u8>` and `()`. Static bodies are not
copied. Streaming and `set_body` are mutually exclusive.

### 3.3 Server

```rust
let server = Server::builder()
    .bind("0.0.0.0:8080")?
    .tls(TlsServerConfig::from_pem(CERT, KEY)?)
    .http1(true)
    .register_header::<XRequestId>()
    .limits(Limits::default())
    .timeouts(Timeouts::default())
    .build()?;
let handle = server.handle();
server.run(my_handler)?;
```

`ServerBuilder`: `bind`, `tls`, `http1`, `limits`, `timeouts`, `register_header`,
`on_error`, `build`. `build` binds TCP sockets, validates config, and returns `Server`.
`run` consumes it and blocks until shutdown. `ServerHandle`: `shutdown`,
`shutdown_with_deadline`, `stats`, `runtime`, `local_addrs`.

```rust
pub trait Handler: Send + Sync + 'static {
    fn handle(&self, req: &mut Request, res: &mut Response) -> Result<(), Error>;
}
impl<F> Handler for F where F: Fn(&mut Request, &mut Response) -> Result<(), Error>
    + Send + Sync + 'static {}
```

`Request`: `method`, `target`, `version`, `headers`, `header::<T>`, `headers_of::<T>`,
`header_value`, `body`, `trailers`, `peer_addr`, `local_addr`, `is_secure`, `is_canceled`,
`defer`, `defer_with_fallback`.
`Response`: `new`, `status`, `set_status`, `headers`, `set<T>`, `append<T>`, `remove<T>`,
`header`, `remove_header`, `body`, `set_body`, `is_canceled`.

Panics are caught: `500` if the head is uncommitted, otherwise reset/close. Handler `Err` is
`500` if uncommitted. The response head is committed at the first body write, at `finish`, or
on handler return. Handlers cannot set framing fields (`Content-Length`, `Transfer-Encoding`,
`Connection`) through typed/raw setters for the library-managed values; those are rejected at
commit with `InvalidRequest`. `HEAD`/`1xx`/`204`/`304` bodies are discarded and no body framing
is emitted. Trailers require chunked framing. Unread request body: h1 drains up to 64 KiB
within 100 ms, then closes with `Connection: close`; if the whole body was already buffered,
keep-alive may continue.

Deferred completion:

```rust
Request::defer(&mut self, &mut Response) -> Result<(DeferredRequest, DeferredResponse), Error>
Request::defer_with_fallback<F>(&mut self, &mut Response, fallback: F)
    -> Result<(DeferredRequest, DeferredResponse), Error>
where F: FnOnce(&mut Response, &DeferredFailure) -> Result<(), Error> + Send + 'static;

pub struct DeferredFailure {
    pub reason: DeferredFailureReason,
    pub method: Method, pub target: String, pub version: Version,
    pub elapsed: Duration, pub request_body_consumed: bool,
}
pub enum DeferredFailureReason {
    Timeout(TimeoutKind), Shutdown, ApplicationError, Dropped, Internal,
}
```

`defer` atomically moves the request head/body and the uncommitted response into `Send` (not
`Sync`) handles, leaving tombstones. It fails `Overloaded` at `max_deferred_exchanges` or
`InvalidRequest` if the response is already committed/body set; on failure nothing moves. The
handler must return promptly after success; a later return value or panic cancels the exchange
and never commits the tombstone. Defaults for a terminal uncommitted response: `504` timeout,
`503` shutdown, `500` application error/drop/internal. A fallback hook runs at most once on a
worker, receives a fresh `Response` and `&DeferredFailure`, and its committed bytes win; a hook
error/panic before commit falls back to the default. If the head is committed or the peer is
gone, the hook is not called and the exchange resets/closes. h1 parks the connection while a
deferred exchange is live. `Timeouts::deferred` defaults to 60 s. Handles may outlive
`Server::run`; operations then return `Canceled`.

### 3.4 Client

```rust
pub struct Client;                          // Clone + Send + Sync
impl Client {
    pub fn builder() -> ClientBuilder;
    pub fn send(&self, request: ClientRequest) -> Result<ClientResponse, Error>;
    pub fn send_with<F>(&self, request: ClientRequest, body: F) -> Result<ClientResponse, Error>
        where F: FnOnce(&mut BodyWriter) -> Result<(), Error>;
    pub fn fetch<F>(&self, request: ClientRequest, on_response: F) -> Result<(), Error>
        where F: for<'a> FnOnce(Result<&'a mut ClientResponse, Error>) -> Result<(), Error>
            + Send + 'static;
    pub fn fetch_with<B, F>(&self, request: ClientRequest, upload: B, on_response: F)
        -> Result<(), Error>
        where B: FnOnce(&mut BodyWriter) -> Result<(), Error> + Send + 'static,
              F: for<'a> FnOnce(Result<&'a mut ClientResponse, Error>) -> Result<(), Error>
                  + Send + 'static;
    pub fn handle(&self) -> ClientHandle;
}
```

`send`/`send_with` block the calling thread through the response head. `fetch`/`fetch_with`
return after enqueue and deliver exactly one callback on a runtime worker; the runtime owns the
response and drops it after the callback. Pre-enqueue failures (`Overloaded`, `Closed`,
`InvalidConfig`) are returned by the verb. A callback/upload `Err` or panic is reported to
`ClientBuilder::on_error`; upload/callback jobs are capped by `max_client_callbacks`,
separately from handlers.

`ClientRequest`: `get`, `post`, `method`, `set<T>`, `append<T>`, `header<T>`, `raw_header`,
`body`, `timeout`, `cancel_with`. `ClientResponse`: `status`, `version`, `headers`,
`header<T>`, `headers_of<T>`, `header_value`, `body`, `trailers`, `peer_addr`, `local_addr`,
`is_secure`, `is_canceled`. `PoolConfig` fields: `max_connections` (1024),
`max_inflight_calls` (4096), `max_queued_body_bytes` (64 MiB), `max_idle_connections` (256),
`max_idle_per_host` (4), `max_connections_per_host` (8), `max_cached_hosts` (1024),
`max_queued_calls` (1024), `queue_timeout` (30 s), `idle_timeout` (60 s).

Pool key is `(scheme, host, port)`. h1 opens up to 8 connections per host, then queues. DNS
runs on resolver threads with a 60 s bounded LRU cache; addresses are tried sequentially with
the per-attempt connect timeout in v1 (Happy Eyeballs is a post-1.0 optimization). One retry
only: reused connection, before response bytes, idempotent method, replayable body. No
redirects, cookies, compression, CONNECT or 0-RTT.

### 3.5 TLS

`TlsServerConfig`: `from_pem(cert, key)`, `from_pem_files(cert_path, key_path)`, optional
client-auth roots. `TlsClientConfig`: `with_webpki_roots`, `with_roots_pem`, optional client
certificate. rustls types are never public. One certificate configuration per listener; no SNI
switching, no 0-RTT, no session-ticket policy. ALPN is `http/1.1`. `close_notify` is sent on
graceful close and truncation is an error.

### 3.6 Limits, timeouts, stats

`Limits` (server) keeps the h1-relevant fields of the frozen API: `max_head_bytes` 16 KiB,
`max_header_count` 100, `max_body_bytes` 8 MiB, `max_connections` 100_000,
`max_connections_per_reactor` 50_000, `max_pending_accepts` 1_024,
`max_inflight_exchanges`/`max_queued_exchanges` (worker-derived), `max_inflight_exchanges_per_connection`
(1 for h1), `max_pipelined_requests` 8, `max_request_body_buffer`/`max_response_body_buffer`
64 KiB, `max_trailer_bytes` 8 KiB, `max_header_arena_bytes` 64 KiB,
`max_connection_buffer_bytes` 256 KiB, `max_buffered_bytes` 256 MiB, `max_pinned_bytes`
64 MiB, `max_pinned_bytes_per_exchange` 128 KiB, `max_parked_workers`/`max_parked_per_connection`
(worker-derived), `max_parked_readers_per_exchange`/`max_parked_writers_per_exchange` 1,
`max_deferred_exchanges` 4 096. h2/h3-specific fields are removed.

`Timeouts`: `header_read` 10 s, `body_idle` 30 s, `read_stall` 15 s, `write_stall` 15 s,
`keep_alive` 60 s, `tls_handshake` 10 s, `connect` 10 s, `total_request` off, `deferred`
60 s, `shutdown_grace` 30 s. One reactor timer authority; workers wait without their own
timeout and receive the terminal outcome.

`Stats`/`ClientStats`/`RuntimeStats` are cheap relaxed-atomic snapshots. There is no public
per-operation cost API.

---

## 4. Semantics that must not drift

- No blocking socket I/O on a reactor; no user code on a reactor; no thread per connection.
- Every queue/ring is bounded with a defined reaction: backpressure, `Overloaded`, a typed
  error, or close.
- Malformed input never panics: bad request line/headers/framing -> `400`/`431`/`413`;
  unsupported expectation -> `417`; body/framing conflict -> close/`400`.
- Request smuggling defenses: reject CL+TE, duplicate CL, obs-fold, whitespace before colon,
  bare CR/LF, invalid chunk sizes/extensions, over-limit heads/trailers, undrained bodies.
- Both directions honor deadlines; cancellation and shutdown wake blocked body operations with
  `Canceled`/`Timeout(kind)`.
- The reactor owns socket/dns-free state; workers own only handles. Dropping a response without
  finishing closes h1 or resets the stream-equivalent.
- No third-party type is reachable through the public API.

---

## 5. Dependencies (internal only)

`mio` (os-poll/net), `socket2` (all), `httparse`, `bytes`, `rustls` (std, tls12, ring;
default features off), `rustls-pemfile`, `webpki-roots`, `libc` (Unix, for `MSG_NOSIGNAL`/
`SO_NOSIGPIPE`). Dev-only: `rcgen` (test certificates), `tempfile`/`proptest` if needed. `http`, `url`,
`httpdate`, `thiserror` and `crossbeam-channel` are used internally; their types never appear
in the public API. No `hyper`, `h2`, `tokio`, or `async-std` in the product or public surface.
A C toolchain is required for `ring`.

`unsafe` is confined to the Unix signal/socket-option shim; the crate is
`#![forbid(unsafe_op_in_unsafe_fn)]`, no public `unsafe`.

---

## 6. Milestones and verification

- **M0** fresh repo, frozen design, CI, crate skeleton.
- **M1** typed model, headers, URI/target, config/errors, body primitives (unit-tested).
- **M2** mio reactor, timers, slab, waker, runtime/workers, bounded rings.
- **M3** strict h1 engine (parser/serializer) + smuggling corpus.
- **M4** h1 server: listener, handler, keep-alive, pipelining, bodies, trailers,
  `100-continue`, deferred completion, shutdown.
- **M5** h1 client: pool, DNS, four verbs, callbacks, cancellation, retry.
- **M6** TLS server/client, ALPN, truncation/close-notify tests.
- **M7** hardening: loopback integration matrix, fuzz seeds, examples, rustdoc, benchmark,
  CI on Linux/macOS/Windows + MSRV, cross-target checks.

Release gate: `cargo test --all-targets`, `cargo fmt --check`,
`cargo clippy --all-targets -- -D warnings`, CI green on the three release platforms, and
`cargo doc` clean. Performance is measured and published but is not a release gate in v1.0;
the post-1.0 roadmap owns hyper-relative gates.

**v1.0 compromises recorded here:** sequential (correctly ordered) h1 pipelining, sequential
DNS address attempts, one acceptor reactor in a multi-reactor runtime, typed headers copied
into owned values at handoff, no zero-copy head parsing, no automatic `Date` generation.

---

## 7. Builder-review resolutions (locked 2026-10-28)

These close the ten blocking findings from the design review. They are part of the contract.

1. **Deferred handles.** `DeferredRequest` exposes the request-head accessors plus
   `body(&mut self) -> &mut BodyReader`, `trailers`, and `is_canceled`.
   `DeferredResponse` exposes `status`, `set_status`, `headers`, `set<T>`, `append<T>`,
   `remove<T>`, `header`, `body(&mut self) -> &mut BodyWriter`, `set_body`,
   `finish`, `finish_with_trailers`, `send`, `send_body`, and `is_canceled`.
   Success transfers ownership; the handler's `Ok(())` by itself neither cancels nor completes
   the exchange, and `Err`/panic after defer is `ApplicationError`. The response is terminal on
   `finish`/`finish_with_trailers`/`send`/`send_body`; dropping an uncommitted
   `DeferredResponse` runs the fallback (default `500`), dropping after commit closes the
   connection. Dropping `DeferredRequest` before body EOF sets `request_body_consumed=false` and
   closes the connection after the response. Both handles hold `Arc` references; after
   `Server::run` returns, body/send operations return `Canceled`, and the fallback runs before
   workers are joined. Successful defer releases its `max_deferred_exchanges` slot when the
   response reaches a terminal state, not when the handler returns.
2. **Persistence is a head-commit decision.** At the first response-head commit, if the request
   body is not fully consumed and not fully buffered, the library emits `Connection: close` and
   will close after the response; this is true even if the handler later reads the body. After
   the head is committed, body-limit/`Err`/panic/fallback failures can only truncate and close,
   never promise keep-alive. `413`/`417`/`431` are sent only while the head is uncommitted; the
   64 KiB/100 ms drain is only used on that uncommitted path. Streaming a response before
   consuming the request body deliberately forfeits keep-alive.
3. **Early final response.** If a final (non-1xx) response head arrives before the client upload
   finishes, the exchange sets `request_aborted`; all subsequent `BodyWriter` writes/flushes
   return `Closed` immediately, the upload closure unwinds, and `send_with`/`fetch_with` deliver
   the response (not the abort error). Any other upload error wins and closes/resets the
   connection. `fetch_with` cancels the upload job and still delivers exactly one callback.
4. **Retries.** `ClientRequest::body` is buffered owned bytes (`Vec<u8>`, `String`,
   `&'static [u8]`, `()`), therefore replayable. `send_with`/`fetch_with` closures are one-shot:
   retry is allowed only if the closure has not yet been invoked. Once any one-shot body byte is
   produced, no retry. Other conditions remain: reused connection, no response bytes, idempotent
   method.
5. **Client limits.** The client enforces the fixed defaults 16 KiB response head, 100 header
   fields, 8 KiB trailers, and a 1 KiB chunk-size/extension line, using the same
   `HeadTooLarge`/`Protocol`/`Trailer` mapping as the server. These are not configurable in
   v1.0. Body size remains caller-bounded by `read_to_end(limit)`.
6. **Interim responses.** `Response::set_status` may store any `StatusCode`, but a handler-set
   `100..=199` is rejected at commit as `InvalidRequest`; a 1xx is never the final response.
   The only interim response is the library-generated `100 Continue`: on the first request-body
   read with `Expect: 100-continue`, if the response head is not committed, the worker/engine
   emits exactly one `100 Continue` before delivering body bytes; if a final response commits
   first, no 100 is sent and rule 2 applies. The client does not send `Expect` in v1.0 and
   uploads immediately.
7. **Client callbacks and shutdown.** Callback/upload jobs share the runtime worker pool; the
   `max_client_callbacks` budget caps concurrency but reserves no thread. When a runtime also
   hosts a server, the effective callback cap is clamped to `max(1, workers - 1)` so one worker
   remains for handlers; `ServerBuilder::build` (not `Runtime::server`) rejects `workers == 0`
   with `InvalidConfig`. Shutdown order: stop admission and return `Closed` for new calls; fail
   queued calls; cancel in-flight exchanges (body ops return `Canceled`); deliver exactly one
   `Err(Canceled)`/`Err(Closed)` callback per accepted fetch on a worker, bounded by
   `shutdown_grace`; report undeliverable callbacks through `on_error`; then join workers.
   `ClientHandle` has `shutdown`, `shutdown_with_deadline`, `stats`, and `runtime`.
8. **Pinned chunks.** `next_chunk` is non-blocking admission: if the per-exchange or global pin
   quota would be exceeded, it returns `Overloaded` immediately and never waits. Dropping a
   `BodyChunk` releases its charge. `read_to_end` can hit this; it is documented.
9. **DNS.** Resolution runs on the resolver pool with a 60 s cache bounded by
   `max_cached_hosts` (1,024 entries). The caller's wait for a lookup is bounded by
   `Timeouts::connect` (10 s); expiry returns `Timeout(Connect)` and the late result is ignored.
   A resolver thread blocked in the platform resolver is detached and replaced up to
   `resolver_threads` times; when no replacement can be made, new calls return `Overloaded`.
   DNS is explicitly best-effort at shutdown because the platform resolver is not cancellable.
10. **Client TLS.** `ClientBuilder::tls(TlsClientConfig)` attaches TLS.
    `TlsServerConfig::with_client_auth_roots(roots, required)` configures server-side client
    auth. `TlsClientConfig::with_client_certificate(cert, key)` configures a client certificate.
    `https` requires a `TlsClientConfig`; otherwise the call returns `InvalidConfig`. `http` is
    never TLS. SNI and verification use the URI host (IP literals use `IpAddress`), ALPN is
    `http/1.1` on both sides, and `close_notify` is sent on graceful close and pool eviction.
    No new dependencies.

**Locked.** Any further change is a scope change and must be recorded in `STATUS.md` with the
reason. Implementation choices not named here are the builder's.
