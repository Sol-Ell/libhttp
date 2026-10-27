# libhttp

A synchronous Rust HTTP library — not a framework — with a sans-I/O protocol core and native
readiness/completion transports on Linux, macOS, FreeBSD and Windows.

The public HTTP API is not implemented yet. The repository currently contains the **M1 transport
spine**: refcounted 16 KiB blocks and a bounded pool, generation-checked timers, cross-thread
wakeup, socket/connection state, a handler seam for echo tests, and epoll/kqueue/IOCP reactor
backends. The full server/client design lives in `docs/superpowers/specs/`.

## Status

- M1 transport spine: in progress (Tasks 1–9 complete; backpressure/close and outbound connect
  follow).
- M2+ HTTP server/client, TLS, HTTP/2 and HTTP/3: design complete, implementation not started.

## Requirements

- Rust 1.85 or newer, edition 2024.
- No async runtime, no `tokio`, no `hyper`, and no thread-per-connection.

## Build and test

```sh
cargo test --all-targets
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

Platform-specific reactor tests are gated by target:

- Linux: epoll
- macOS/FreeBSD: kqueue
- Windows: IOCP

## Design

- [Top-level design](docs/superpowers/specs/2026-10-26-libhttp-sync-http-design.md)
- [Runtime/transport design](docs/superpowers/specs/2026-10-26-libhttp-runtime-design.md)
- [Execution design](docs/superpowers/specs/2026-10-26-libhttp-execution-design.md)
- [Protocols design](docs/superpowers/specs/2026-10-26-libhttp-protocols-design.md)
- [Client design](docs/superpowers/specs/2026-10-26-libhttp-client-design.md)
- [M1 implementation plan](docs/superpowers/plans/2026-10-26-libhttp-transport-spine.md)
