# libhttp — Client Design

**Status:** part of the design for approval. **Date:** 2026-10-26.
**Parent:** [2026-10-26-libhttp-sync-http-design.md](2026-10-26-libhttp-sync-http-design.md).
**Companions:** [runtime](2026-10-26-libhttp-runtime-design.md), [execution](2026-10-26-libhttp-execution-design.md),
[protocols](2026-10-26-libhttp-protocols-design.md).

The client is a blocking request/response front-end over the same `Runtime` as the server. A call is
an exchange, not a completion object: the reactor performs protocol I/O, the calling thread waits
through the response head, and the resulting `ClientResponse` owns the streamed body. There is no
async runtime, `Future`, `Waker`, callback executor, call table, or `ClientCall`.

---

## 1. Surface

```rust
let rt = Runtime::builder()
    .reactors(2)                  // default: min(available_parallelism(), 4)
    .workers(8)                   // default: available_parallelism(); lazy for client-only
    .resolver_threads(1)          // default: 1; lazy for server-only
    .build()?;

let client = rt.client()
    .tls(TlsClientConfig::with_webpki_roots())
    .pool(PoolConfig::default())
    .timeouts(Timeouts::default())
    .http1(true)
    .http2(true)                  // ALPN order h2, http/1.1
    .h2c(false)                   // plaintext prior knowledge only
    .http3(false)                 // explicit; no Alt-Svc/fallback
    .max_response_body_bytes(None)
    .register_header::<XRequestId>()
    .on_error(Arc::new(|e| eprintln!("client error: {e}")))
    .build()?;

let handle = client.handle();     // shutdown + stats

// Buffered/empty request body; blocks through the response head.
let mut res = client.send(ClientRequest::get(uri))?;
while res.body().read(&mut buf)? != 0 { /* ... */ }

// Streaming upload; `finish` blocks and returns the response.
let mut ex = client.send_streaming(ClientRequest::post(uri))?;
ex.body().write_all(b"first\n")?;
ex.body().finish()?;
let mut res = ex.finish()?;
```

`Client` is `Clone + Send + Sync`; `ClientRequest` is consumed by `send`/`send_streaming`;
`ClientResponse` is `Send`; `ClientExchange` is `Send`, not `Sync`.

```rust
impl Client {
    pub fn builder() -> ClientBuilder;                 // private Runtime
    pub fn send(&self, request: ClientRequest) -> Result<ClientResponse, Error>;
    pub fn send_streaming(&self, request: ClientRequest) -> Result<ClientExchange, Error>;
    pub fn handle(&self) -> ClientHandle;
}

impl ClientExchange {
    pub fn body(&mut self) -> &mut BodyWriter;
    pub fn finish(self) -> Result<ClientResponse, Error>;
    pub fn cancel(self);
}
```

- `send` accepts a buffered or empty body. It blocks the calling thread on DNS, pool/stream
  capacity, connect/TLS, upload, and response headers. It returns the `ClientResponse` as soon as
  the response head is parsed; the body is streamed by the caller.
- `send_streaming` rejects a `ClientRequest` whose body is already set with `InvalidRequest`; it
  returns an exchange whose `BodyWriter` is the upload. `finish` implicitly finishes a still-open
  body writer, blocks for the response head, and returns it. `cancel` aborts the exchange; body writes fail with `Canceled`.
- `send_streaming` is not a second completion model: it is the upload path. There is no
  `wait`/`on_complete`/`try_wait`/`wait_timeout`; `send` and `finish` are the two completion points.
- Cancellation is pre-armed through the request (below) or `ClientExchange::cancel`; `send` has no
  handle after it starts, so a caller that needs cancellation arms a token first.

### 1.1 Request and cancellation

```rust
impl ClientRequest {
    pub fn get(uri: Uri) -> Self;
    pub fn post(uri: Uri) -> Self;
    pub fn method(method: Method, uri: Uri) -> Self;
    pub fn set<T: Header>(self, value: T) -> Self;
    pub fn append<T: Header>(self, value: T) -> Result<Self, HeaderError>;
    pub fn header<T: Header>(self, value: T) -> Self;
    pub fn body(self, body: impl IntoRequestBody) -> Self;
    pub fn timeout(self, timeout: Duration) -> Self;       // Timeout(Total)
    pub fn cancel_token(self, token: CancelToken) -> Self;
}

pub struct CancelToken;                                    // Clone + Send + Sync
impl CancelToken {
    pub fn new() -> Self;
    pub fn cancel(&self);
    pub fn is_canceled(&self) -> bool;
}
```

- A token can be armed before `send`; cancel from another thread wakes the caller/queue/DNS and
  prevents a queued or resolving call from connecting. `send_streaming` also observes it.
- `Drop` on a request/call cancels it. A `send` caller cannot be cancelled without a token.
- Request timeout is the total call deadline; pool queue and body/write stalls have their own
  reactor deadlines. There is no caller-side `wait_timeout`; `send` is the wait.

---

## 2. Shared runtime

`Runtime` is the only execution owner:

```rust
pub struct Runtime;                                        // Clone + Send + Sync
impl Runtime {
    pub fn builder() -> RuntimeBuilder;
    pub fn server(&self) -> ServerBuilder;                 // attaches, does not own
    pub fn client(&self) -> ClientBuilder;                 // attaches, does not own
    pub fn handle(&self) -> RuntimeHandle;
}

impl RuntimeBuilder {
    pub fn reactors(self, n: usize) -> Self;               // default min(available_parallelism(), 4)
    pub fn workers(self, n: usize) -> Self;                // default available_parallelism()
    pub fn resolver_threads(self, n: usize) -> Self;       // default 1
    pub fn build(self) -> Result<Runtime, Error>;
}

impl RuntimeHandle {
    pub fn shutdown(&self);
    pub fn shutdown_with_deadline(&self, grace: Duration);
    pub fn wait(&self) -> Result<(), Error>;
    pub fn stats(&self) -> RuntimeStats;
}
```

- Runtime owns reactors/transports, the worker pool, resolver threads, block/timer pools and
  admission. `Server` and `Client` are front-ends; they add listeners, handler pools, client pools
  and protocol configuration, not threads.
- `rt.server()`/`rt.client()` attach. `Server::builder()`/`Client::builder()` create a private
  runtime and are sugar for the standalone case.
- A handler captures a `Client` clone built before the server; no thread-local/client injection:
  ```rust
  let rt = Runtime::builder().workers(8).build()?;
  let client = rt.client().tls(...).build()?;
  let outbound = client.clone();
  let server = rt.server().bind("0.0.0.0:8080")?.handler(move |req, res| {
      // Prefer Request::defer for a long upstream call; the app thread then uses outbound.send.
      let mut up = outbound.send(ClientRequest::get(backend))?;
      // ...
      Ok(())
  }).build()?;
  ```
  A handler that calls `send` directly blocks a worker; a handler that defers frees the worker and
  performs the blocking `send` on its application thread. There is no library callback to run.
- Worker and resolver pools start lazily: a client-only runtime starts reactors + resolvers, not
  workers; a server-only runtime starts reactors + workers, not resolvers. `RuntimeHandle` is the
  root shutdown; `ServerHandle`/`ClientHandle` stop only their component and leave the runtime usable.
- Failure isolation is a second `Runtime`; there is no hidden per-component worker pool.
- `ServerHandle::runtime()` and `ClientHandle::runtime()` return the shared runtime for composition
  and tests.

---

## 3. Pool, queue and resolution

```rust
pub struct PoolConfig {
    pub max_connections: usize,                // 1,024 global, all hosts/protocols
    pub max_inflight_calls: usize,             // 4,096 queued + active
    pub max_queued_body_bytes: usize,          // 64 MiB buffered request bodies in the queue
    pub max_idle_connections: usize,           // 256 global LRU
    pub max_idle_per_host: usize,              // 4
    pub max_h1_connections_per_host: usize,    // 8
    pub max_h2_connections_per_host: usize,    // 2
    pub max_h3_connections_per_host: usize,    // 2
    pub max_cached_hosts: usize,               // 1,024 DNS cache entries, LRU
    pub max_queued_calls: usize,               // 1,024
    pub queue_timeout: Duration,               // 30 s
    pub idle_timeout: Duration,                // 60 s
}
```

- Pool key: `(scheme, host, port, ALPN)`. No certificate coalescing.
- h1: one in-flight request per connection, up to 8 per host; h2/h3: multiplex up to the peer's
  stream limit, open another connection up to 2 when all are saturated, then queue. A global
  `max_connections` and `max_inflight_calls` bound all hosts.
- `send` blocks in the queue; full queue returns `Overloaded`; expiry is `Timeout(PoolQueue)`.
  `max_queued_body_bytes` counts buffered request bodies held by queued/resolving/connecting calls;
  a body that does not fit returns `Overloaded`. Streaming upload bodies are caller-owned and not
  queued in memory.
- Idle connections retire after `idle_timeout`; a dead reused connection is discarded and the call
  retries under §4.
- DNS runs on the runtime's resolver threads, always off the caller and reactor, with a 60 s cache
  bounded by `max_cached_hosts` (LRU). Addresses are tried with Happy Eyeballs: start one attempt,
  stagger the next address after 250 ms, first success wins, the rest are cancelled.
- IPv4/IPv6 and ALPN are recorded per connection; a failed attempt closes its socket before the next.

---

## 4. Retries

One retry is allowed when **all** hold:

- the failing connection was reused (not a fresh connect),
- the failure happened before any response bytes arrived,
- the method is idempotent (`GET`, `HEAD`, `PUT`, `DELETE`, `OPTIONS`, `TRACE`),
- the body is replayable (buffered/owned; `send_streaming` is not replayable).

The retry is internal to `send`/`finish`; the caller sees only the final result. No retry budget, no
backoff loop, and no retry after response bytes or for a fresh-connection failure.

---

## 5. Timeouts and limits

| Item | Default | Behavior |
|---|---|---|
| queue | `PoolConfig::queue_timeout`, 30 s | `Timeout(PoolQueue)` |
| connect per attempt / overall | `Timeouts::connect`, 10 s | `Timeout(Connect)` |
| TLS handshake | `Timeouts::tls_handshake`, 10 s | `Timeout(TlsHandshake)` |
| response headers | `Timeouts::header_read`, 10 s | `Timeout(HeaderRead)` |
| response body idle | `Timeouts::body_idle`, 30 s | `Timeout(BodyIdle)` |
| response read stall | `Timeouts::read_stall`, 15 s | `Timeout(ReadStall)` |
| request body write stall | `Timeouts::write_stall`, 15 s | `Timeout(WriteStall)` |
| total call | request timeout or `Timeouts::total_request`, off | `Timeout(Total)` |
| response body size | `max_response_body_bytes`, `None` | `BodyTooLarge` mid-stream if set |

- `ClientRequest::timeout` overrides `Timeouts::total_request` for that request; the earliest
  deadline wins. There is no separate caller wait timeout.
- Client request bodies are caller-owned and are not limited by the server's `max_body_bytes`.
  Buffered bodies are already in memory; streaming bodies are caller-owned and bounded by the caller
  and the request deadline. Only responses have `max_response_body_bytes`.
- Response header limits are the fixed protocol defaults (`max_header_count`/`max_head_bytes` for h1;
  64 KiB including 32 bytes/field and the QPACK budget for h2/h3); the typed-header arena is 64 KiB
  per in-flight response, bounded by the pool/stream caps and `max_inflight_calls`.
- Dropping a `ClientResponse` before EOF closes the HTTP/1.1 connection or resets the HTTP/2/3
  stream.

---

## 6. Typed headers and bodies

Requests and responses use the same `Header` trait as the server:

```rust
let req = ClientRequest::get(uri)
    .set(Accept::text())
    .header(header::X_REQUEST_ID, "abc")?;
let ct = res.headers().get::<ContentType>();
```

- Application response headers need `ClientBuilder::register_header::<T>()`; unregistered fields stay
  raw. Outgoing typed request headers need no registration.
- Typed request encode runs on the calling thread before the call is queued. Typed response decode
  runs on the thread that called `send`/`finish`, before `ClientResponse` is returned.
- `send` uses a buffered/empty body; `send_streaming` provides the `BodyWriter`. Response bodies are
  always streamed through `BodyReader`.
- Early final response: if the server answers before the upload finishes, body writes fail with
  `Canceled`, `finish` returns that response, and the response is not lost.
- `Expect: 100-continue` is not sent; the body is written immediately.

---

## 7. Lifecycle

- `ClientHandle::shutdown` cancels queued, resolving, connecting and in-flight calls, including
  response bodies already handed out (reads return `Canceled`/`Closed`), stops accepting new calls
  (`Closed`), and joins the client's connections. `shutdown_with_deadline` gives in-flight calls a
  grace period. `Client`/`ClientHandle`/`ClientResponse` hold refcounted leases; a late response
  cannot touch a freed connection.
- A private runtime dies with its last client/handle; a shared runtime stays alive when a component
  shuts down and is stopped by `RuntimeHandle::shutdown` (which cancels both front-ends).
- Resolver threads cannot be portably cancelled inside `getaddrinfo`: at shutdown they detach and
  late results are ignored; they hold only the runtime's resolver mailbox, never call state.
- `ClientHandle::stats()` returns a relaxed snapshot: calls started/active/queued, connections by
  protocol, timeouts, resets, bytes. No per-operation cost API.
- Dropping an unfinished `ClientExchange` cancels it. `send` is cancellable only via the request
  token; a `ClientResponse` body read after shutdown returns `Closed`.

---

## 8. Testing API

The library provides a small, feature-gated harness. `testing` is off by default and has no
test-only branch in the core.

```rust
#[cfg(feature = "testing")]
pub mod testing {
    pub struct TestServer;
    impl TestServer {
        pub fn builder() -> TestServerBuilder;
        pub fn addr(&self) -> SocketAddr;
        pub fn udp_addr(&self) -> Option<SocketAddr>;
        pub fn handle(&self) -> ServerHandle;
        pub fn shutdown(self) -> Result<(), Error>;     // deterministic; Drop is best-effort
    }
    impl TestServerBuilder {
        pub fn handler<H: Handler>(self, handler: H) -> Self;
        pub fn runtime(self, runtime: &Runtime) -> Self;
        pub fn tls(self, tls: TlsServerConfig) -> Self;
        pub fn http1(self, on: bool) -> Self;
        pub fn http2(self, on: bool) -> Self;
        pub fn http3(self, on: bool) -> Self;
        pub fn spawn(self) -> Result<TestServer, Error>; // 127.0.0.1:0, waits ready
    }
}
```

- `spawn` binds TCP port 0 (and the same UDP port when h3 is enabled), starts `Server::run` on a
  thread, and blocks until `ServerHandle::wait_ready`. TLS is caller-supplied; the harness does not
  generate certificates.
- Tests use a normal `Client` on the same runtime:
  ```rust
  let test = TestServer::builder().handler(h).spawn()?;
  let client = test.handle().runtime().client().tls(...).build()?;
  let res = client.send(ClientRequest::get(format!("https://{}", test.addr()).parse()?))?;
  test.shutdown()?;
  ```
- There is no `TestClient` and no fake transport: tests exercise the real loopback TCP/UDP,
  reactors, TLS, ALPN and h1/h2/h3. The harness only removes ephemeral-port/readiness/teardown
  boilerplate.
- Why provide it: combining server and client in tests is the primary reason for the shared
  runtime; without the harness every user repeats the same brittle setup. The feature gate keeps
  production and default builds free of test surface.

---

## 9. Test matrix

- `send` buffered/empty/streaming upload; `finish`; early response during upload; request timeout.
- `CancelToken` cancels queued, resolving, connecting and in-flight; a resolving cancel never sends.
- Shutdown cancels calls and response readers; later calls are `Closed`; runtime/component
  lifecycle; private vs shared runtime.
- Queue full (`Overloaded`), queue timeout, h1/h2/h3 per-host caps, global connection/in-flight/body
  caps, h2 stream saturation opening a second connection.
- Resolver cache/Happy Eyeballs; no caller blocking on DNS; late resolver result after shutdown.
- Retry on reused connection, no retry on streaming body, no retry after response bytes.
- Typed headers, trailers, body drop/reset, `max_response_body_bytes`, error mapping
  (`PoolQueue/Connect/TlsHandshake/HeaderRead/BodyIdle/ReadStall/WriteStall/Total`, `Canceled`,
  `Closed`, `Overloaded`, `Header`, `Protocol`, `Tls`, `Io`).
- `testing` feature: `TestServer` spawn/ready/shutdown, TCP+UDP, TLS/h2/h3.

The client never follows redirects, keeps cookies, decompresses content, proxies CONNECT, upgrades,
uses HTTP/2 push, discovers Alt-Svc, uses 0-RTT, migrates connections, or coalesces connections by
certificate. Those belong above the transport.
