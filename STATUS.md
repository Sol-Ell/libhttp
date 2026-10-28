# Status

**Goal:** ship libhttp v1.0 — HTTP/1.1 server + client + TLS, frozen by `docs/DESIGN.md`.
**Rule:** no HTTP/2, HTTP/3, proxy, or public low-level I/O. Record compromises here, not in
the design.

## Milestones

- [ ] M0 — fresh repo, frozen design, CI, crate skeleton
- [ ] M1 — typed model, headers, URI/target, config/errors, body primitives
- [ ] M2 — mio reactor, timers, slab, waker, runtime/workers, bounded rings
- [ ] M3 — strict h1 engine + smuggling corpus
- [ ] M4 — h1 server: listener, handler, keep-alive, pipelining, bodies, trailers, 100-continue,
      deferred completion, shutdown
- [ ] M5 — h1 client: pool, DNS, four verbs, callbacks, cancellation, retry
- [ ] M6 — TLS server/client, ALPN, truncation/close-notify
- [ ] M7 — hardening, examples, rustdoc, benchmark, CI

## In progress

M0.

## Compromises / decisions

- Sequential (ordered) h1 pipelining; no concurrent handler execution per connection.
- Sequential DNS address attempts; Happy Eyeballs is post-1.0.
- One acceptor reactor in a multi-reactor runtime; accepted connections are handed off where
  the platform permits, otherwise kept on the acceptor.
- Header values are copied into owned storage at handoff; no zero-copy head parsing.
- No automatic `Date` generation.
- Performance is measured and published, but hyper-relative gates are post-1.0.
