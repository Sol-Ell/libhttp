# libhttp

[![CI](https://github.com/Sol-Ell/libhttp/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Sol-Ell/libhttp/actions/workflows/ci.yml)

A synchronous Rust HTTP/1.1 server and client library. Not a framework, not an async runtime.

`libhttp` runs ordinary functions as handlers on a bounded worker pool. Reactor threads own
sockets and never run user code. Bodies are read and written through blocking, deadline-aware
handles. There is no `Future`, `Waker`, executor, hidden async runtime, or
thread-per-connection.

**v1.0 scope:** HTTP/1.1 server and client, streaming and buffered bodies, trailers, keep-alive,
ordered pipelining, `100-continue`, typed headers, deferred completion, TLS via rustls, and
graceful shutdown. HTTP/2 and HTTP/3 are post-1.0.

The frozen contract is [docs/DESIGN.md](docs/DESIGN.md). Live progress and the remaining
compromises are in [STATUS.md](STATUS.md).

## Server

```rust
use libhttp::Runtime;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::builder().build()?;
    let server = runtime.server().bind("127.0.0.1:8080")?.build()?;
    server.run(
        |_req: &mut libhttp::server::Request,
         res: &mut libhttp::server::Response| {
            res.set_body("hello\n");
            Ok(())
        },
    )?;
    Ok(())
}
```

## Client

```rust
use libhttp::client::ClientRequest;
use libhttp::{Client, Uri};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder().build()?;
    let uri: Uri = "http://example.com/".parse()?;
    let mut response = client.send(ClientRequest::get(uri))?;
    let body = response.body().read_to_end(1 << 20)?;
    println!("{}", String::from_utf8_lossy(&body));
    Ok(())
}
```

## Build and test

```sh
cargo test --all-targets
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps --all-features
```

Rust 1.85 or newer, edition 2024. TLS needs a C toolchain because rustls uses `ring`.

## Examples

- `cargo run --example hello`
- `cargo run --example echo`
- `cargo run --example client`

## Design and security

- No blocking socket I/O on a reactor; no handler runs on a reactor.
- Every queue and body ring is bounded. Oversized/hostile input closes or gets a typed error.
- Request-smuggling defenses: CL+TE, duplicate CL, obs-fold, whitespace before colon, bare
  CR/LF, invalid chunk sizes, bounded trailers.
- Public types belong to `libhttp`; `httparse`, `bytes`, `http`, `url`, `mio`, `socket2` and
  `rustls` are implementation details.
