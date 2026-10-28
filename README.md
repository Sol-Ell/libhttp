# libhttp

A synchronous Rust HTTP/1.1 server and client library. Not a framework, not an async runtime.

`libhttp` runs ordinary functions as handlers on a bounded worker pool. Reactor threads own
sockets and never run user code. Bodies are read and written through blocking, deadline-aware
handles. There is no `Future`, `Waker`, executor, hidden async runtime, or thread-per-connection.

**v1.0 scope:** HTTP/1.1 server and client, streaming bodies, typed headers, keep-alive,
pipelining, TLS via rustls, deferred completion, cancellation, and graceful shutdown on Linux,
macOS and Windows. HTTP/2 and HTTP/3 are post-1.0.

The frozen contract is [docs/DESIGN.md](docs/DESIGN.md). Live progress is in
[STATUS.md](STATUS.md).

## Build

```sh
cargo test --all-targets
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

Rust 1.85 or newer, edition 2024. TLS needs a C toolchain because rustls uses `ring`.

## Status

Pre-release. See [STATUS.md](STATUS.md) for the current milestone.
