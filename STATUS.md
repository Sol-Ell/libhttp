# Status

**Goal:** ship libhttp v1.0 — HTTP/1.1 server + client + TLS, frozen by `docs/DESIGN.md`.
**Rule:** no HTTP/2, HTTP/3, proxy, or public low-level I/O.

## Milestones

- [x] M0 — fresh repo, frozen/locked design, CI, crate skeleton
- [x] M1 — typed model, headers, URI/target, config/errors, body primitives
- [x] M2 — mio reactor, timers, slab, waker, runtime/workers, bounded rings
- [x] M3 — strict h1 engine + smuggling corpus
- [x] M4 — h1 server: listener, handler, keep-alive, ordered pipelining, bodies, trailers,
      100-continue, deferred completion, shutdown
- [x] M5 — h1 client: pool, blocking verbs, callback verbs, streaming upload, retry
- [x] M6 — TLS server/client, ALPN http/1.1, truncation/close-notify
- [x] M7 — hardening, examples, rustdoc, CI release gate (performance harness post-1.0)

## Current verification

- `cargo test --all-targets --all-features` — 97 tests (unit + loopback server/client/TLS/
  deferred/feature integration tests).
- `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo doc --no-deps --all-features` — clean.
- MSRV 1.85 check passes.
- CI matrix: Ubuntu, macOS, Windows; lint, docs, feature builds, MSRV.

## Compromises / decisions

These are deliberate v1.0 choices; they do not change the public API.

- Client I/O is blocking on the calling/worker thread, with one background pump thread per
  in-flight response body. The server is reactor-driven; the client deliberately trades
  reactor complexity for a small, auditable client. `resolver_threads`/`max_client_callbacks`
  are accepted but not yet used as separate pools.
- Ordered h1 pipelining is processed sequentially per connection; no concurrent handler
  execution on one connection.
- An unread request body closes the connection after the response instead of draining up to
  64 KiB. `max_body_bytes` still returns 413 before the response head is committed.
- Deferred timeout sends 504; shutdown closes rather than running the custom fallback hook.
  The hook runs on drop before commit.
- HTTP/1.0 responses and streamed h1.0 bodies use close-delimited framing; 1.1 uses chunked.
- Header values/names are copied into owned storage at handoff; no zero-copy head parsing.
- Static response bodies are held as `Bytes::from_static` but are copied into the outbound
  ring; no `writev` zero-copy path yet.
- No automatic `Date` generation.
- Client early-final-response handling is best-effort: an upload error gets a short chance to
  read an already-arrived final response; otherwise the upload error wins.
- `max_buffered_bytes` is enforced per exchange rather than as a server-wide low-water
  eviction loop. `max_pending_accepts` and `max_pipelined_requests` are conservative bounds,
  not queues.
- DNS resolution uses the platform resolver on the caller/worker thread, bounded by socket
  timeouts; Happy Eyeballs and the separate resolver pool are post-1.0.
- No benchmark harness is shipped in v1.0; hyper-relative performance gates are post-1.0.
