# libhttp — Client Design

**Status:** part of the design for approval. **Date:** 2026-10-26.
**Parent:** [2026-10-26-libhttp-sync-http-design.md](2026-10-26-libhttp-sync-http-design.md).
**Companions:** [runtime](2026-10-26-libhttp-runtime-design.md), [execution](2026-10-26-libhttp-execution-design.md),
[protocols](2026-10-26-libhttp-protocols-design.md).

The client is a synchronous transport with two surfaces: a blocking convenience call and a
non-blocking `ClientCall` that can be waited on, polled, cancelled, or completed through an explicit
application executor. There is no async runtime, `Future`, `Waker`, or library-owned executor.

---

## 1. Surface

```rust
let client = Client::builder()
    .tls(TlsClientConfig::with_webpki_roots())
    .pool(PoolConfig::default())
    .reactor_threads(1)
    .resolver_threads(1)
    .timeouts(Timeouts::default())
    .http1(true)
    .http2(true)          // ALPN order h2, http/1.1; h2c only with .h2c(true)
    .h2c(false)           // plaintext HTTP/2 prior knowledge
    .http3(false)         // explicit; no Alt-Svc, no fallback
    .max_response_body_bytes(None)
    .register_header::<XRequestId>() // application typed response header
    .on_error(Arc::new(|e| eprintln!("client error: {e}")))
    .build()?;

let handle: ClientHandle = client.handle();   // shutdown + stats

// Blocking convenience:
let mut res = client.send(request)?;
while res.body().read(&mut buf)? != 0 { /* ... */ }

// Non-blocking:
let mut call = client.start(request)?;
let cancel = call.cancel_handle();
let mut res = call.wait()?;                    // or try_wait/wait_timeout

// Streaming upload, then wait:
let mut call = client.start_streaming(request)?;
{
    let body = call.body().expect("streaming call");
    body.write_all(b"first\n")?;
    body.finish()?;
}
let mut res = call.wait()?;

// Completion on an application executor (no library thread waits):
let call = client.start(request)?;
call.on_complete(executor.clone(), move |result| {
    match result { Ok(resp) => { /* runs on executor's thread */ }, Err(e) => { /* ... */ } }
})?;

handle.shutdown_with_deadline(Duration::from_secs(5));
```

`Client` is `Clone + Send + Sync`; calls are `ClientCall`. `ClientCall` is `Send` but not `Sync`;
`ClientResponse` and `BodyReader` are `Send` (the callback owns them); `CancelHandle` and
`ClientHandle` are `Clone + Send + Sync`.

```rust
impl Client {
    pub fn builder() -> ClientBuilder;
    pub fn start(&self, request: ClientRequest) -> Result<ClientCall, Error>;
    pub fn start_streaming(&self, request: ClientRequest) -> Result<ClientCall, Error>;
    pub fn send(&self, request: ClientRequest) -> Result<ClientResponse, Error>;
    pub fn send_with<F>(&self, request: ClientRequest, body: F) -> Result<ClientResponse, Error>
    where F: FnOnce(&mut BodyWriter) -> Result<(), Error>;
    pub fn handle(&self) -> ClientHandle;
}
```

### 1.1 `ClientCall`

```rust
impl ClientCall {
    pub fn body(&mut self) -> Option<&mut BodyWriter>; // start_streaming only
    pub fn cancel_handle(&self) -> CancelHandle;
    pub fn is_finished(&self) -> bool;
    pub fn try_wait(&mut self) -> Result<Option<ClientResponse>, Error>;
    pub fn wait(self) -> Result<ClientResponse, Error>;
    pub fn wait_timeout(self, timeout: Duration) -> Result<ClientResponse, Error>;
    pub fn on_complete(
        self,
        executor: Arc<dyn Executor>,
        f: impl FnOnce(Result<ClientResponse, Error>) + Send + 'static,
    ) -> Result<(), Error>;
}

impl CancelHandle {
    pub fn cancel(&self);
}

pub trait Executor: Send + Sync + 'static {
    fn execute(&self, job: Box<dyn FnOnce() + Send + 'static>);
}
```

- `start`/`start_streaming` never block on DNS, connect, a pool slot, or a stream slot; they enqueue
  a call and return `Overloaded` immediately only when the queue itself is full.
- `wait` blocks the calling thread; `try_wait` never blocks and returns the response at most once
  (later polls return `Closed`); `wait_timeout` blocks at most its timeout and cancels the call on
  expiry (`Timeout(Wait)`). The earliest of `wait_timeout` and the configured call deadlines wins.
- `on_complete` stores an `Arc<dyn Executor>` in the job and submits exactly one job. The executor
  must accept the job, hand it to another thread, never run it inline, never block in `execute`, and
  not panic;
  a bounded executor must reserve capacity before calling `on_complete`. With that contract
  `on_complete` cannot lose a completion; it returns `Closed` only if the call was already consumed.
  If the call is already settled, `on_complete` submits from the calling thread. The callback,
  typed-header decode and body reads then run off the client reactor. The library never creates or
  owns an executor.
- Completion is a single atomic transition. If the reactor settles first, `on_complete` observes
  `Settled` and submits the callback from the calling thread; if `on_complete` installs first, the
  reactor submits exactly once. There is no "submit only from the reactor" path that can hang.
  `try_wait` handing out the response consumes the result; later `wait`/`on_complete` return
  `Closed`, and two owners can never receive the same body.
- `wait`/`wait_timeout`/`on_complete` hand out the response at most once. A call that has already
  been consumed is `Closed`.
- `Drop` on an unfinished `ClientCall` cancels it. `wait`/`wait_timeout`/`on_complete` implicitly
  finish a still-open request body writer; the body is sent as written. `body()` returns `None` for a
  buffered call or after the writer is finished.
- `cancel` from another thread wakes waiters and delivers `Canceled`; queued calls are removed,
  resolving calls are marked canceled and their resolver result is dropped, connecting calls are
  aborted, and in-flight requests are reset/closed. A canceled resolving call **never** reaches
  connect/TLS/write, so a non-idempotent request cannot be sent after the caller has seen
  `Canceled`/`Timeout(Wait)`.

### 1.2 `Client` and `ClientHandle`

- `Client::handle()` returns a `ClientHandle` with `shutdown()`,
  `shutdown_with_deadline(Duration)`, and `stats() -> ClientStats`.
- `shutdown` cancels queued, resolving, connecting and in-flight calls, including response bodies
  already handed to the caller (their reads return `Canceled`), stops accepting new calls
  (`Closed`), joins the reactors and returns. `shutdown_with_deadline` gives in-flight calls that
  grace before cancellation. Resolver threads cannot be portably cancelled inside `getaddrinfo`:
  they are detached at the shutdown deadline and own only a separate `Arc<ResolverMailbox>`
  (resolution cache + atomic shutdown flag), never the client/call state. A late result is posted to
  the mailbox and dropped if the flag is set. A resolver thread that never returns delays process
  exit but never blocks shutdown or causes a use-after-free.
- `Client`, `ClientCall`, `ClientHandle`, `CancelHandle`, `ClientResponse` and any queued callback
  job each hold a refcounted call/connection lease and count as live handles. Dropping the last one
  performs an immediate shutdown and joins the reactors (resolver threads detach as described above);
  explicit shutdown is recommended for deterministic teardown. After shutdown, `cancel` is a no-op
  and a response-body read returns `Closed`/`Canceled`, never a freed connection. A call after
  shutdown returns `Closed`.
- `ClientStats` is a relaxed-atomic snapshot: calls started/active/queued, connections by protocol,
  timeouts, resets, bytes. No per-operation cost API.

---

## 2. Call lifecycle, pool and queue

Call states: `Queued`, `Resolving`, `Connecting`, `Sending`, `AwaitingResponse`, `Done`, `Canceled`.

`PoolConfig`:

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

- Pool key: `(scheme, host, port, ALPN)`. No certificate-based coalescing. `max_connections` is a
  global cap across every host and protocol; `max_idle_connections` evicts the least-recently-used
  idle connections globally, and `max_idle_per_host` caps each host. `max_cached_hosts` bounds the
  resolver cache; the oldest entry is evicted first.
- `max_inflight_calls` caps queued + active calls across all hosts; `start` returns `Overloaded` at
  the cap. `max_queued_body_bytes` caps the buffered request bodies held by queued/resolving/
  connecting calls; a body that does not fit returns `Overloaded` rather than pinning unbounded
  caller memory. Streaming bodies are caller-owned and are not queued in memory.
- HTTP/1.1: one in-flight request per connection; open up to
  `max_h1_connections_per_host`; then queue.
- HTTP/2: multiplex up to the peer's `MAX_CONCURRENT_STREAMS`. When every permitted h2 connection is
  at its stream limit, open another up to `max_h2_connections_per_host`; then queue. This removes the
  single-connection bottleneck without abandoning multiplexing.
- HTTP/3: same policy with `max_h3_connections_per_host`.
- `start` enqueues a call; a full `max_queued_calls` returns `Overloaded`. `queue_timeout` expires a
  queued call with `Timeout(PoolQueue)`; the timeout is delivered by `wait`/callback, not by blocking
  `start`.
- Idle connections retire after `idle_timeout`; a dead reused connection is discarded and the call
  retries under §4.
- A call may be cancelled while queued, resolving, connecting or in flight.

---

## 3. Resolution and connect

- `reactor_threads` (default 1) sets the number of client reactors; each owns its share of
  connections, and the pool is shared across them. `reactor_threads(0)` and `resolver_threads(0)`
  are rejected at build with `InvalidConfig`. DNS runs on `resolver_threads` (default 1)
  blocking resolver threads, never on a reactor and never on the caller. The resolver queue is
  bounded by `max_inflight_calls`/`max_queued_body_bytes`; results are cached per host in the
  resolver mailbox with a 60 s TTL and refreshed by the resolver thread.
- Addresses are tried with Happy Eyeballs: start one attempt, then stagger the next address after
  250 ms; the first success wins, the rest are cancelled. `Timeouts::connect` bounds each attempt and
  the overall connect phase. Address family and IPv4/IPv6 are recorded for the connection.
- On readiness backends connect is nonblocking + `SO_ERROR`; on Windows `ConnectEx` with
  `SO_UPDATE_CONNECT_CONTEXT`. A failed attempt closes its socket before the next is started.
- TLS follows a successful connect under `Timeouts::tls_handshake`; ALPN selects h2 vs h1.

---

## 4. Retries

One retry is allowed when **all** hold:

- the failing connection was reused (not a fresh connect),
- the failure happened before any response bytes arrived,
- the method is idempotent (`GET`, `HEAD`, `PUT`, `DELETE`, `OPTIONS`, `TRACE`),
- the body is replayable (static/owned; a streaming call is not replayable).

The retry is internal to the call: `wait`/`on_complete` see only the final result. No retry budget,
no backoff loop, and no retry after response bytes or for a fresh-connection failure.

---

## 5. Timeouts and limits

| Item | Default | Behavior |
|---|---|---|
| queue | `PoolConfig::queue_timeout`, 30 s | `Timeout(PoolQueue)` |
| connect per attempt / overall | `Timeouts::connect`, 10 s | `Timeout(Connect)` |
| TLS handshake | `Timeouts::tls_handshake`, 10 s | `Timeout(TlsHandshake)` |
| response headers | `Timeouts::header_read`, 10 s | `Timeout(HeaderRead)` |
| response body idle | `Timeouts::body_idle`, 30 s | `Timeout(BodyIdle)` |
| response per-read stall | `Timeouts::read_stall`, 15 s | `Timeout(ReadStall)` |
| request body write stall | `Timeouts::write_stall`, 15 s | `Timeout(WriteStall)` |
| total call | `Timeouts::total_request`, off | `Timeout(Total)` |
| caller wait | `wait_timeout` argument | `Timeout(Wait)`, call cancelled |
| response body size | `max_response_body_bytes`, `None` | `BodyTooLarge` mid-stream if set |

- Client request bodies are caller-owned and are not limited by the server's `max_body_bytes`.
  Buffered bodies are already in memory; streaming bodies are bounded by the caller and
  `total_request`. Only responses have `max_response_body_bytes`.
- Response header limits are the fixed protocol defaults (`max_header_count`/`max_head_bytes` for h1;
  64 KiB including 32 bytes/field and the QPACK budget for h2/h3); the typed-header arena is 64 KiB
  per in-flight response, bounded by the pool/stream caps and `max_queued_calls`.
- Dropping a `ClientResponse` before EOF closes the HTTP/1.1 connection or resets the HTTP/2/3
  stream.

---

## 6. Typed headers and bodies

Requests and responses use the same `Header` trait as the server:

```rust
req.set(Accept::text());
req.header(header::X_REQUEST_ID, "abc")?;     // raw/custom
let ct = res.headers().get::<ContentType>();
```

- Application response headers need `ClientBuilder::register_header::<T>()`; unregistered fields stay
  raw. Outgoing typed request headers need no registration.
- Typed response decode runs on the calling thread for `wait`, or on the executor thread when the
  callback is invoked. Typed request encode runs on the thread that sets the body/waits.
- Request bodies: `start(request)` uses a buffered/empty body; `start_streaming(request)` gives a
  `BodyWriter` on the call and rejects a request whose body is already set with `InvalidRequest`.
  Response bodies are always streamed through `BodyReader`.
- Early final response: if the server answers before the request body is finished, body writes fail
  with `Canceled`, and the call resolves to that response. `send_with` returns it rather than the
  closure's error.
- `Expect: 100-continue` is not sent by the client; the body is written immediately.

---

## 7. Convenience API

Inside a server handler, prefer `start`/`on_complete`: `send` and `send_with` block the calling
worker. `send(request)` is `start(request)?.wait()`. `send_with(request, body_fn)` starts a streaming call,
runs the closure on the calling thread, finishes the body, and waits; it returns the early response
if the server responded first (a closure error is then discarded). If the closure fails before a
response, the call is cancelled and `send_with` returns that error. Both are conveniences over
`ClientCall`; they do not bypass its timeouts, cancellation or lifecycle.

`Client` does not follow redirects, keep cookies, decompress content, proxy CONNECT, upgrade, use
HTTP/2 push, discover Alt-Svc, use 0-RTT, migrate connections, or coalesce connections by
certificate. It is a transport; those policies belong above it.

---

## 8. Test matrix

- `start`/`send`/`start_streaming`/`on_complete` against a loopback server; the callback runs on the
  executor thread and a callback that blocks does not stall a client reactor.
- `try_wait` pending/done; `wait_timeout` cancels; `CancelHandle` cancels queued/resolving/in-flight;
  a resolving cancel never connects or sends.
- `on_complete` after the call already settled, and after `try_wait` has handed out the response;
  exactly one callback/response owner.
- `max_inflight_calls` and `max_queued_body_bytes` return `Overloaded` at the cap.
- Resolver shutdown flag drops a late DNS result; `CancelHandle` after shutdown is a no-op and a
  response-body read is `Closed`.
- `shutdown` cancels calls, joins threads, later calls are `Closed`; dropping the last handle joins.
- Queue full (`Overloaded`), queue timeout, h1 connection cap, h2 stream saturation opening a second
  connection, h3 cap, global `max_connections` cap and global idle LRU eviction, DNS cache LRU.
- Resolver thread with a slow/multi-address host; Happy Eyeballs starts the second attempt; no caller
  blocking.
- Retry on a reused connection, no retry on a streaming body, no retry after response bytes.
- Early response during upload; typed headers; trailers; response body drop/reset.
- Error mapping: `Timeout(PoolQueue/Connect/TlsHandshake/HeaderRead/BodyIdle/ReadStall/WriteStall/Total/Wait)`,
  `Canceled`, `Closed`, `Overloaded`, `Header`, `Protocol`, `Tls`, `Io`.
