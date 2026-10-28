# Changelog

## 1.0.0 — 2026-10-28

First complete release.

- Synchronous HTTP/1.1 server and client with no async runtime, `Future`, `Waker`, executor,
  hidden runtime, or thread-per-connection.
- Server: bounded worker pool, keep-alive, ordered pipelining, streaming and fixed bodies,
  chunked transfer, trailers, `100-continue`, deferred completion with a terminal fallback,
  graceful shutdown, typed and raw headers, smuggling defenses.
- Client: blocking `send`/`send_with`, callback `fetch`/`fetch_with`, buffered and chunked
  upload, streaming response bodies, connection pool, one retry for reused idempotent calls,
  cancellation token, timeouts, response-size limits.
- TLS for HTTP/1.1 via rustls (`ring`), ALPN `http/1.1`, PEM server certs, PEM/webpki client
  roots, optional client certificates, `close_notify`.
- Platform transport via mio: epoll (Linux), kqueue (macOS/BSD), IOCP (Windows).
- MSRV 1.85, edition 2024.

Known v1.0 limitations are documented in [STATUS.md](STATUS.md).
