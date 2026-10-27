# libhttp — Testing Design

**Status:** part of the design for approval. **Date:** 2026-10-26.
**Parent:** [2026-10-26-libhttp-sync-http-design.md](2026-10-26-libhttp-sync-http-design.md).
**Companions:** [runtime](2026-10-26-libhttp-runtime-design.md), [execution](2026-10-26-libhttp-execution-design.md),
[protocols](2026-10-26-libhttp-protocols-design.md), [client](2026-10-26-libhttp-client-design.md).

The library provides a small, feature-gated harness for tests that combine a server and a client.
It is not a mock HTTP implementation: it starts the normal `Server` on loopback and tests use the
normal `Client` on the same `Runtime`. The harness removes only ephemeral-port, readiness and
teardown boilerplate.

---

## 1. Feature and module

- Feature `testing`, off by default; it requires `server` and `client`.
- `libhttp::testing::{TestServer, TestServerBuilder}`.
- No `TestClient`, no fake transport, no test-only branch in the reactor or protocol engines.
- No certificate generation. TLS tests supply their own fixtures through `TestServerBuilder::tls`.

The feature exists because the repeated brittle parts of an in-process test are binding an ephemeral
port, waiting until the listener is actually ready, starting the server without racing the client,
and tearing it down deterministically. The HTTP behavior itself is never replaced.

---

## 2. Surface

```rust
#[cfg(feature = "testing")]
pub mod testing {
    pub struct TestServer;
    pub struct TestServerBuilder;

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
        pub fn spawn(self) -> Result<TestServer, Error>;
    }
}
```

Usage:

```rust
let test = TestServer::builder()
    .handler(my_handler)
    .http1(true)
    .http2(true)
    .spawn()?;                       // 127.0.0.1:0, waits until ready

let client = test.handle().runtime().client()
    .http1(true)
    .http2(true)
    .build()?;

let mut res = client.send(ClientRequest::get(
    format!("http://{}/health", test.addr()).parse()?,
))?;
assert_eq!(res.status(), StatusCode::OK);

test.shutdown()?;                    // joins the server thread
```

For h3, `.tls(...)` is required, `addr()` is the TCP address and `udp_addr()` is the UDP address
bound on the same port.

---

## 3. Semantics

- `spawn` binds `127.0.0.1:0` for TCP and, when h3 is enabled, the same port for UDP. It starts
  `Server::run` on a dedicated thread and blocks until `ServerHandle::wait_ready` succeeds. If the
  server fails before readiness, `spawn` returns the error and joins the thread.
- `handler`, `tls`, `http1/2/3` have the same meaning as on `ServerBuilder`; `http3` without TLS is
  `InvalidConfig`. The harness does not add protocol behavior.
- A supplied `runtime` is used for both server and client (`handle().runtime()`), so a combined test
  has one reactor/worker/resolver set. Without one, the server gets a private runtime and the test
  client should use `handle().runtime()` to share it.
- `shutdown` signals the server, waits for in-flight work under the server's shutdown grace, joins
  the server thread, and returns. `Drop` is best-effort: it signals but does not panic or block
  indefinitely.
- The test owns the server thread; the client is a normal `Client` and is not implicitly shut down
  by `TestServer::shutdown`. Drop clients first or let their handles drop; a private runtime is
  joined when its last handle drops.
- `TestServer` is `Send` but not necessarily `Sync`; use it from the test's controlling thread.

---

## 4. Test matrix

- spawn/ready/addr for TCP; UDP address only with h3; TLS-required h3.
- Combined server + client on one runtime; no second reactor/worker/resolver set.
- Deterministic shutdown; in-flight request finishes within grace; later calls are `Closed`.
- h1, h2 (TLS/ALPN and h2c), h3; streaming upload/download; typed headers/trailers.
- TLS fixtures supplied by the caller; no generated certificates.
- A server that fails to bind/start makes `spawn` return an error rather than hanging.
