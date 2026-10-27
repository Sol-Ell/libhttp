# libhttp — Client Design

**Status:** part of the design for approval. **Date:** 2026-10-26.
**Parent:** [2026-10-26-libhttp-sync-http-design.md](2026-10-26-libhttp-sync-http-design.md).
**Companions:** [runtime](2026-10-26-libhttp-runtime-design.md), [execution](2026-10-26-libhttp-execution-design.md),
[protocols](2026-10-26-libhttp-protocols-design.md).

The client is a set of blocking and callback verbs over the same `Runtime` as the server. A call is
an exchange, not a completion object: the reactor performs protocol I/O, a blocking verb waits on
the calling thread, and a callback verb receives an owned-by-the-runtime `ClientResponse` by mutable
reference on a runtime worker. There is no async runtime, `Future`, `Waker`, application executor,
call table, or `ClientCall`.

---

## 1. Surface

```rust
let client = rt.client()          // Runtime::client(); Client::builder() uses a private runtime
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

let handle = client.handle();

// Blocking, buffered/empty body.
let mut res = client.send(ClientRequest::get(uri))?;

// Blocking, streaming upload.
let mut res = client.send_with(ClientRequest::post(uri), |body| {
    body.write_all(b"first\n")?;
    body.finish()
})?;

// Nonblocking. The callback runs on a runtime worker when the head arrives.
client.fetch(ClientRequest::get(uri), |result| {
    match result {
        Ok(response) => { /* response: &mut ClientResponse; stream its body */ Ok(()) }
        Err(error) => Err(error),
    }
})?;

// Nonblocking, streaming upload. Upload runs as a worker job; the callback follows.
client.fetch_with(ClientRequest::post(uri),
    |body| { body.write_all(b"first\n")?; body.finish() },
    |result| match result {
        Ok(response) => { /* ... */ Ok(()) }
        Err(error) => Err(error),
    })?;
```

`Client` is `Clone + Send + Sync`. `ClientRequest` is consumed by every verb; `ClientResponse` is
`Send`; callbacks are `Send + 'static`.

```rust
impl Client {
    pub fn builder() -> ClientBuilder;                 // private Runtime
    pub fn send(&self, request: ClientRequest) -> Result<ClientResponse, Error>;
    pub fn send_with<F>(&self, request: ClientRequest, body: F) -> Result<ClientResponse, Error>
    where F: FnOnce(&mut BodyWriter) -> Result<(), Error>;

    pub fn fetch<F>(&self, request: ClientRequest, on_response: F) -> Result<(), Error>
    where
        F: for<'a> FnOnce(Result<&'a mut ClientResponse, Error>) -> Result<(), Error>
            + Send + 'static;

    pub fn fetch_with<B, F>(
        &self,
        request: ClientRequest,
        upload: B,
        on_response: F,
    ) -> Result<(), Error>
    where
        B: FnOnce(&mut BodyWriter) -> Result<(), Error> + Send + 'static,
        F: for<'a> FnOnce(Result<&'a mut ClientResponse, Error>) -> Result<(), Error>
            + Send + 'static;

    pub fn handle(&self) -> ClientHandle;
}
```

### 1.1 The four verbs

| Verb | Completion | Request body |
|---|---|---|
| `send` | blocks the calling thread through the response head | buffered/empty |
| `send_with` | blocks the calling thread through upload and response head | streaming closure on caller |
| `fetch` | returns after enqueue; callback on a runtime worker | buffered/empty |
| `fetch_with` | returns after enqueue; upload then callback on runtime workers | streaming closure on worker |

This is a systematic 2×2, not a collection of aliases. Each axis changes where work runs and what
can be expressed; there is no fifth convenience wrapper.

- `send`/`send_with` return the final response or error. They are for application threads. Calling
  them inside a handler blocks that worker; use `fetch`/`fetch_with` or `Request::defer` + an
  application thread.
- `fetch`/`fetch_with` return only enqueue/queue-admission errors (`Overloaded`, `Closed`).
  Everything after enqueue — DNS, connect, TLS, rejection, timeout, cancellation, callback
  processing error — is delivered to the one callback. The callback runs exactly once.
- The callback receives `Result<&mut ClientResponse, Error>`, not an owned response. The runtime
  owns the `ClientResponse` and drops it after the callback; the callback may stream the body but
  cannot retain or move the response. Dropping it unread closes/resets as usual.
- A callback `Err` is reported through `ClientBuilder::on_error`; the response is dropped/reset. A
  panic in a callback or upload closure is caught by the worker and reported the same way.
- `fetch`/`fetch_with` never hold a worker while waiting for DNS/connect/TLS/response; a worker is
  occupied only for the upload closure and for the response callback.

### 1.2 Request and cancellation

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
    pub fn cancel_with(self, token: &CancelToken) -> Self;
}

pub struct CancelToken;                                    // Clone + Send + Sync
impl CancelToken {
    pub fn new() -> Self;
    pub fn cancel(&self);
    pub fn is_canceled(&self) -> bool;
}
```

- A token is armed before the verb. It cancels a queued/resolving/connecting/in-flight call from
  another thread; a canceled resolving call never connects or sends.
- `send`/`send_with` cannot be cancelled without a token because they have no call handle.
  `fetch`/`fetch_with` also rely on the token; their only post-enqueue handle is the callback.
- Request timeout is the total call deadline. There is no separate caller wait timeout.

---

## 2. Runtime and admission

The client owns no threads. `Runtime` (runtime design) owns reactors, workers, resolver threads,
block/timer pools and admission; `Server` and `Client` are front-ends. `rt.client()` attaches;
`Client::builder()` creates a private runtime. Multiple clients on one runtime share reactors and
workers.

- A blocking `send`/`send_with` blocks its caller and consumes no library worker.
- `fetch`/`fetch_with` callbacks and `fetch_with` uploads are worker jobs; the first such call starts
  the runtime worker pool if it is not already running. They are bounded by
  `RuntimeBuilder::max_client_callbacks` (default `max(1, workers / 2)`), separate from handler
  dispatch, so a flood of callbacks cannot starve handlers. A full callback budget returns
  `Overloaded` from the verb.
- A server handler should use `fetch`/`fetch_with` and complete a `DeferredResponse` from the
  callback; no application executor or thread is needed. A blocking `send` is for application
  threads or after `Request::defer`.
- Runtime shutdown cancels client calls and delivers `Err(Canceled)` to their callbacks.
  `ClientHandle::shutdown` stops only client connections and leaves the runtime usable.

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
- A verb blocks in the queue (blocking verbs) or the callback waits; full queue returns
  `Overloaded`; expiry is `Timeout(PoolQueue)`. `max_queued_body_bytes` counts buffered request
  bodies held by queued/resolving/connecting calls; a body that does not fit returns `Overloaded`.
  Streaming upload bodies are worker/caller-owned and not queued in memory.
- Idle connections retire after `idle_timeout`; a dead reused connection is discarded and the call
  retries under §5.
- DNS runs on the runtime resolver threads, off the caller and reactor, with a 60 s cache bounded by
  `max_cached_hosts` (LRU). Addresses are tried with Happy Eyeballs: start one attempt, stagger the
  next address after 250 ms, first success wins, the rest are cancelled.
- IPv4/IPv6 and ALPN are recorded per connection; a failed attempt closes its socket before the next.

---

## 4. Timeouts and limits

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
  deadline wins.
- Client request bodies are caller-owned and are not limited by the server's `max_body_bytes`.
  Buffered bodies are already in memory; streaming bodies are caller/worker-owned and bounded by the
  caller and the request deadline. Only responses have `max_response_body_bytes`.
- Response header limits are the fixed protocol defaults (`max_header_count`/`max_head_bytes` for h1;
  64 KiB including 32 bytes/field and the QPACK budget for h2/h3); the typed-header arena is 64 KiB
  per in-flight response, bounded by the pool/stream caps and `max_inflight_calls`.
- Dropping a `ClientResponse` before EOF closes the HTTP/1.1 connection or resets the HTTP/2/3
  stream. In a `fetch` callback this is the runtime dropping it after the callback returns; a
  callback that wants the body must read it before returning.

---

## 5. Retries

One retry is allowed when **all** hold:

- the failing connection was reused (not a fresh connect),
- the failure happened before any response bytes arrived,
- the method is idempotent (`GET`, `HEAD`, `PUT`, `DELETE`, `OPTIONS`, `TRACE`),
- the body is replayable (buffered/owned; `send_with`/`fetch_with` are not replayable).

The retry is internal to the verb; the caller/callback sees only the final result. No retry budget,
no backoff loop, and no retry after response bytes or for a fresh-connection failure.

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
- Typed request encode runs on the thread that calls the verb, before enqueue (the `fetch_with`
  upload closure is body-only). Typed response decode runs on the `send`/`send_with` caller or the
  `fetch`/`fetch_with` callback's worker before the response is handed over.
- `send`/`fetch` use a buffered/empty body; `send_with`/`fetch_with` provide the `BodyWriter`.
  A still-open writer is finished implicitly when the closure returns. Response bodies are always
  streamed through `BodyReader`.
- Early final response: if the server answers before the upload finishes, body writes fail with
  `Canceled`; `send_with` returns that response directly, and `fetch_with` passes it to the normal
  callback. The response is not lost because the local write failed.
- `Expect: 100-continue` is not sent; the body is written immediately.

---

## 7. Lifecycle

- `ClientHandle::shutdown` cancels queued, resolving, connecting and in-flight calls, including
  response bodies already handed out (reads return `Canceled`/`Closed`), stops accepting new calls
  (`Closed`), and closes the client's connections. `shutdown_with_deadline` gives in-flight calls a
  grace period. `Client`/`ClientHandle`/`ClientResponse` hold refcounted leases; a late response
  cannot touch a freed connection.
- A private runtime dies with its last client/handle; a shared runtime stays alive when a component
  shuts down and is stopped by `RuntimeHandle::shutdown`.
- Resolver threads cannot be portably cancelled inside `getaddrinfo`: at shutdown they detach and
  late results are ignored; they hold only the runtime resolver mailbox, never call state.
- `ClientHandle::stats()` returns a relaxed snapshot: calls started/active/queued, connections by
  protocol, timeouts, resets, bytes. No per-operation cost API.
- A `fetch` callback that outlives the client? It cannot: the callback is invoked before the job is
  released; shutdown delivers `Err(Canceled)` if the callback slot was reserved. The callback owns
  no response after returning.

---

## 8. Test matrix

- `send`, `send_with`, `fetch`, `fetch_with` against a loopback server; callback once and only once.
- Callback receives `Err` for DNS/connect/TLS/timeout/rejection/cancel; callback `Err` reaches
  `on_error`; response is dropped/reset.
- `CancelToken` cancels queued, resolving, connecting and in-flight; a resolving cancel never sends.
- Shutdown cancels calls and response readers; later calls are `Closed`; runtime/component
  lifecycle; private vs shared runtime.
- Queue full (`Overloaded`), queue timeout, callback-budget exhaustion, h1/h2/h3 per-host caps,
  global connection/in-flight/body caps, h2 stream saturation opening a second connection.
- Resolver cache/Happy Eyeballs; no caller blocking on DNS; late resolver result after shutdown.
- Retry on reused connection, no retry on streaming body, no retry after response bytes.
- Typed headers, trailers, body drop/reset, `max_response_body_bytes`, error mapping
  (`PoolQueue/Connect/TlsHandshake/HeaderRead/BodyIdle/ReadStall/WriteStall/Total`, `Canceled`,
  `Closed`, `Overloaded`, `Header`, `Protocol`, `Tls`, `Io`).

The client never follows redirects, keeps cookies, decompresses content, proxies CONNECT, upgrades,
uses HTTP/2 push, discovers Alt-Svc, uses 0-RTT, migrates connections, or coalesces connections by
certificate. Those belong above the transport.
