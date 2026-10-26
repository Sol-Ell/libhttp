# libhttp — Client Design

**Status:** part of the design for approval. **Date:** 2026-10-26.
**Parent:** [2026-10-26-libhttp-sync-http-design.md](2026-10-26-libhttp-sync-http-design.md).
**Companions:** [runtime](2026-10-26-libhttp-runtime-design.md), [execution](2026-10-26-libhttp-execution-design.md),
[protocols](2026-10-26-libhttp-protocols-design.md).

The client is a synchronous transport, not a browser: no redirects, cookies, compression, or
automatic protocol discovery beyond ALPN/h2c configuration.

---

## 1. Surface

```rust
let client = Client::builder()
    .tls(TlsClientConfig::with_webpki_roots())
    .connect_timeout(Duration::from_secs(10))
    .pool(PoolConfig::default())
    .http1(true)
    .http2(true)        // ALPN order h2, http/1.1; h2c only with .h2c(true)
    .h2c(false)         // plaintext HTTP/2 prior knowledge
    .http3(false)       // explicit; no Alt-Svc, no fallback
    .timeouts(Timeouts::default())   // client-side header/body/read/total/shutdown timers
    .max_response_body_bytes(None)
    .register_header::<XRequestId>() // application typed response header
    .on_error(Arc::new(|e| eprintln!("client error: {e}")))
    .build()?;

let mut req = client.request(Method::GET, "https://example.com/path")?;
req.set(Accept::text());                      // typed standard header
req.header(header::X_REQUEST_ID, "abc")?;     // raw/custom header
req.body(&b"hello"[..]);                      // buffered body
req.timeout(Duration::from_secs(30));         // optional total

let mut res = client.send(req)?;
let mut buf = [0u8; 8192];
while res.body().read(&mut buf)? != 0 { /* ... */ }

let res = client.send_with(req, |body| body.write_all(b"chunked upload"))?;
```

- `Client` is `Clone + Send + Sync`; each call blocks the calling thread.
- A `Client` owns its reactors (default one, configurable) and its pool; it never borrows a server's
  reactors.
- `ClientRequest`: `set<T: Header>`, `append<T: Header>` (returns `Result`, rejects singleton duplicates), `header` (raw), `body`, `timeout`;
  `ClientResponse`: `status`, `version`, `headers` (typed `get::<T>()`/raw `header_value`), `body`,
  `trailers`.
- Response typed headers are decoded on the calling thread when `send`/`send_with` returns; a bad
  known header is a `HeaderError` before the response is handed over. `register_header::<T>()` is
  required for application typed response headers; unregistered fields stay raw. Outgoing typed
  request headers are encoded on the calling thread before the request is submitted, so the client
  reactor only sees raw values.
- Per-request `timeout` overrides `Timeouts::total_request` for that request.
- Invalid header names/values return `Error::InvalidRequest`; they never panic.
- `.h2c(true)` without `.http2(true)` is rejected at build with `Error::InvalidConfig`.
- `Timeouts` is defined at the crate root (`libhttp::Timeouts`) and is the same type server and client use;
  `.connect_timeout(d)` is shorthand for the `connect` field, and the last setter wins.
- Header limits are fixed protocol defaults (h1 `max_header_count`/`max_head_bytes`; h2/h3 the 64 KiB
  list budget and QPACK budget), not configurable in v1; `ClientBuilder` exposes timeouts, pool,
  protocol selection, response-body limit and the error hook.

Protocol choice:

| Configuration | TLS | Plaintext |
|---|---|---|
| `http1(true), http2(true)` (default) | ALPN `h2`, `http/1.1` | HTTP/1.1 |
| `http2(false)` | ALPN `http/1.1` only | HTTP/1.1 |
| `h2c(true)` | unchanged | HTTP/2 prior knowledge |
| `http3(true)` (requires `http3` + `tls`) | QUIC/h3 only | n/a (h3 is TLS-only) |

---

## 2. Pool and queue semantics

`PoolConfig` defaults:

```rust
pub struct PoolConfig {
    pub max_idle_per_host: usize,        // 4
    pub max_connections_per_host: usize, // 8 for h1; h2/h3 use one multiplexed connection
    pub idle_timeout: Duration,          // 60 s
    pub max_queued_requests: usize,      // 1024
    pub queue_timeout: Duration,         // 30 s
}
```

- Pool key: `(scheme, host, port, ALPN)`. No certificate-based coalescing in v1.
- HTTP/1.1: one in-flight request per connection; idle connections are reused; `Keep-Alive` is
  observed and a `Connection: close` response retires the connection.
- HTTP/2: one connection per authority, multiplexed up to the peer's `MAX_CONCURRENT_STREAMS`;
  exceeding it queues locally rather than opening a second connection (a second connection is opened
  only if the first is GOAWAYed or lost).
- HTTP/3: one QUIC connection per authority, multiplexed up to our advertised stream limit.
- When no slot is free, the caller waits in a bounded queue. A full queue returns
  `Error::Overloaded` immediately. `queue_timeout` expiry returns
  `Error::Timeout(TimeoutKind::PoolQueue)`. This wait is caller-owned and has no reactor timer.
- A handler worker calling a `Client` that needs another handler on the same `Server` can wait up to
  `queue_timeout`: a documented application-level deadlock risk, not hidden or silently mitigated.
- Idle connections are closed after `idle_timeout`; a connection that fails its liveness check before
  use is discarded and replaced.

---

## 3. Name resolution and connect

- Hostnames are resolved on the **calling thread** with the system resolver, never on a reactor.
  Resolved addresses are cached per host with a 60 s TTL by default; the next request refreshes them.
- Multiple addresses are tried sequentially under the connect timeout; no Happy Eyeballs in v1.
- On readiness backends connect is nonblocking + `SO_ERROR`; on Windows `ConnectEx`. The connect
  timeout is a reactor timer that cancels the attempt.
- TLS handshake follows the connection under `Timeouts::tls_handshake`; ALPN decides h2 vs h1.

---

## 4. Retries

One retry is allowed when **all** hold:

- the connection was reused (not a fresh connect),
- the failure happened before any response bytes arrived,
- the method is idempotent (`GET`, `HEAD`, `PUT`, `DELETE`, `OPTIONS`, `TRACE`),
- the body is replayable (static or owned; a `send_with` streaming body is not).

Otherwise the error is returned. There is no retry budget, no backoff loop, and no retry on a fresh
connection failure.

---

## 5. Timeouts and limits

| Item | Default | Behavior |
|---|---|---|
| connect | 10 s | `Timeout(Connect)` |
| TLS handshake | 10 s | `Timeout(TlsHandshake)` |
| response headers | `header_read`, 10 s | `Timeout(HeaderRead)` |
| response body idle | `body_idle`, 30 s | `Timeout(BodyIdle)` |
| per-read stall | `read_stall`, 15 s | `Timeout(ReadStall)` |
| pool queue | 30 s | `Timeout(PoolQueue)` |
| total request | `Timeouts::total_request`, off | `Timeout(Total)` when set |
| response body size | `None` | `BodyTooLarge` mid-stream if set |

- Dropping a `ClientResponse` before EOF closes the HTTP/1.1 connection or resets the HTTP/2/3
  stream.
- Client request bodies are **not** limited by the server's `max_body_bytes`: they are caller-owned
  data. A buffered body is already in memory; a streaming body is bounded by the caller and, if set,
  by `total_request`. There is no client request-body-size knob in v1, and `send` never rejects a
  buffered body for size. Only the response body has a limit (`max_response_body_bytes`, default
  `None`).
- Response header count/size limits are the fixed protocol defaults (`max_header_count` and
  `max_head_bytes` for h1; 64 KiB including 32 bytes/field, and the QPACK budget, for h2/h3); the
  typed-header arena is 64 KiB per in-flight response, bounded by the pool/stream caps and
  `max_queued_requests`. They are not configurable on the client in v1.
- Connection-level idle/h2-GOAWAY handling mirrors the server: GOAWAY retires streams above the
  last-stream-id, which are retried only under §4's rules.
- Early final response: if the server answers before the request body is finished (e.g. `413`), the
  upload is stopped, body writes fail with `Canceled`, and `send_with` returns that early response
  rather than the closure's write error. The response is never lost.

---

## 6. What the client does not do

No redirect following, cookie jar, content decoding, proxy CONNECT, `101` upgrades, HTTP/2 push,
HTTP/3 Alt-Svc discovery, 0-RTT, connection migration, or certificate-coalesced connection reuse. A
caller that needs any of these implements it above `Client`; the transport surface is complete enough
to send and stream bodies and inspect status/headers/trailers.
