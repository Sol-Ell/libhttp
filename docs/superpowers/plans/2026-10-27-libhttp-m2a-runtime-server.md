# libhttp M2a — Runtime and HTTP/1.1 Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the public shared `Runtime`, worker/admission layer and a real HTTP/1.1 server on the M1 transport spine.

**Architecture:** M1's reactors/connections/pools stay private. A public `Runtime` owns reactor, worker, resolver and admission state; `Server` attaches listeners and dispatches exchanges to a bounded worker pool. HTTP/1.1 is a sans-I/O head/body engine; bodies stream through bounded blocks. Deferred completion uses the approved terminal fallback.

**Tech Stack:** Rust 2024, MSRV 1.85, no async runtime, no `tokio`/`hyper`, only `libc`/`windows-sys` as target dependencies. rustls/h2/h3 are out of M2.

**Spec:** `docs/superpowers/specs/2026-10-26-libhttp-sync-http-design.md` plus runtime/execution/protocols designs and `docs/superpowers/plans/2026-10-26-libhttp-transport-spine.md`.

## Global Constraints

- Edition 2024; MSRV 1.85; one published crate `libhttp`.
- No async/`Future`/`Waker`/executor; no blocking socket I/O on reactors; no thread-per-connection.
- Handlers are synchronous and run only on workers.
- Bounded memory: 256 MiB active buffers + 64 MiB pinned chunks; 4,096 inflight calls; 1,024 queued calls; 64 MiB queued body bytes; 64 KiB h1 head; 8 KiB h1 trailers.
- h1 only in M2a; no TLS/h2/h3.
- `Request::defer_with_fallback` uses `FnOnce(&mut Response, &DeferredFailure) -> Result<(), Error>`; no `FallbackResponse`, no separate observer.
- Public API signatures and semantics come from the design docs; no public `unsafe`.

## Review Focus

1. A slow/malicious h1 peer must not make a reactor or worker allocate unbounded memory.
2. Pipelining must preserve request order and never mix bodies/responses.
3. Deferred completion must never double-commit or silently truncate; fallback follows the 504/503/500 map.
4. Shutdown/cancellation must wake blocked body operations and never touch freed rings.
5. Typed/raw header disagreement must never produce smuggling or a handler-visible mismatch.

---

### Task 1: Public HTTP primitives and errors

**Files:** Create `src/http/mod.rs`, `src/http/method.rs`, `src/http/status.rs`, `src/http/version.rs`, `src/http/header.rs`, `src/http/target.rs`, `src/error.rs`; modify `src/lib.rs`.

**Interfaces:** Produces the public `Method`, `StatusCode`, `Version`, `HeaderName`, `HeaderValue`, `HeaderRef`, `HeaderMap`, `Header`, `HeaderError`, `Target`, `Uri`, `Error`, `TimeoutKind` types with the signatures in design §6.4–6.5.

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn method_token_roundtrips() { assert_eq!(Method::GET.as_str(), "GET"); assert!(Method::from_bytes(b"BAD METHOD").is_err()); }
#[test] fn status_rejects_out_of_range() { assert!(StatusCode::from_u16(99).is_err()); assert!(StatusCode::from_u16(600).is_err()); }
#[test] fn header_value_rejects_crlf() { assert!(HeaderValue::try_from("a\r\nb").is_err()); }
#[test] fn typed_header_duplicate_singleton_is_rejected() { /* Set-Cookie multiple; Host singleton */ }
```

- [ ] **Step 2: Run `cargo test --lib http::`** — FAIL unresolved types.
- [ ] **Step 3: Implement token/status/version/header/target/error types; lowercase header names; typed `set` replaces, `append` rejects singleton duplicates; expose root re-exports.**
- [ ] **Step 4: Run `cargo test --lib http:: && cargo clippy --all-targets -- -D warnings`** — PASS.
- [ ] **Step 5: Commit** `feat(http): public primitives and typed headers`.

### Task 2: Public Runtime and lifecycle

**Files:** Create `src/runtime/mod.rs`, `src/runtime/reactor.rs`, `src/runtime/stats.rs`; modify `src/lib.rs`, `src/rt/mod.rs`.

**Interfaces:** Produces `Runtime::{builder,server,client,handle}`, `RuntimeBuilder::{reactors,workers,resolver_threads,max_client_callbacks,build}`, `RuntimeHandle::{shutdown,shutdown_with_deadline,wait,stats}`, `RuntimeStats`; private runtime ownership; lazy worker/resolver pools.

- [ ] **Step 1: Tests: shared runtime survives server shutdown; `reactors(0)` fails; `workers(0)` allows blocking client but server build/fetch fails; handle wait/shutdown; reactor threads stop.**
- [ ] **Step 2: Run focused tests — FAIL unresolved Runtime.**
- [ ] **Step 3: Implement runtime ownership and thread lifecycle; start reactors at build; no listener yet.**
- [ ] **Step 4: Focused/full tests + clippy/fmt.**
- [ ] **Step 5: Commit** `feat(runtime): public shared runtime`.

### Task 3: Worker pool, dispatch and admission

**Files:** Create `src/runtime/worker.rs`, `src/runtime/admission.rs`; modify `src/runtime/mod.rs`.

**Interfaces:** Produces `WorkerPool::{new,dispatch,shutdown}`, bounded dispatch queue, `Admission` counters for inflight/queued exchanges, per-connection caps, cancellation handles. Handlers never run on a reactor.

- [ ] **Step 1: Tests: dispatch runs on a worker; queue bound returns `Overloaded`; shutdown wakes queued jobs; no reactor thread executes handler code; cancellation skips queued work.**
- [ ] **Step 2: Focused test FAIL.**
- [ ] **Step 3: Implement pool/queue/admission with the design defaults (4,096 inflight, 1,024 queued, worker-derived parked caps).**
- [ ] **Step 4: Run tests/clippy/fmt.**
- [ ] **Step 5: Commit** `feat(runtime): bounded worker dispatch`.

### Task 4: HTTP/1.1 head parser and serializer

**Files:** Create `src/http/h1/mod.rs`, `src/http/h1/head.rs`, `src/http/h1/parse.rs`, `src/http/h1/encode.rs`; modify `src/http/mod.rs`.

**Interfaces:** Produces `H1Head`, `parse_request_head(&mut HeadBuf, Limits) -> Result<RequestHead, ProtocolError>`, `parse_response_head`, `encode_response_head`; strict CRLF, request-target forms, Host/absolute authority, no obs-fold, bounded counts/bytes, smuggling rejects.

- [ ] **Step 1: Tests: valid GET; absolute-form Host mismatch `400`; duplicate Content-Length rejected; TE+CL rejected; non-final chunked rejected; 16 KiB head error `431`; response serialization Content-Length/Connection.**
- [ ] **Step 2: Run `cargo test --lib http::h1` — FAIL.**
- [ ] **Step 3: Implement parser/serializer with exactly the design's strictness; no I/O.**
- [ ] **Step 4: Run tests + clippy/fmt.**
- [ ] **Step 5: Commit** `feat(h1): strict head parser and serializer`.

### Task 5: Streaming bodies and limits

**Files:** Create `src/http/body.rs`; modify `src/http/mod.rs`, `src/runtime/admission.rs`.

**Interfaces:** Produces `BodyReader::{read,next_chunk,read_to_end,is_end}`, `BodyWriter::{write,write_all,flush,finish,finish_with_trailers}`, `BodyChunk::as_bytes`; block-backed, bounded, cancellation-aware; trailers limited to 8 KiB.

- [ ] **Step 1: Tests: streaming request body exactly once; `max_body_bytes` error; chunked trailers; response body writer backpressure; cancel wakes blocked read/write; retained `BodyChunk` counts as pinned.**
- [ ] **Step 2: Focused test FAIL.**
- [ ] **Step 3: Implement over M1 blocks/pool/Conn; no reactor blocking; body operations block only the app/worker thread.**
- [ ] **Step 4: Run tests + clippy/fmt.**
- [ ] **Step 5: Commit** `feat(http): streaming bodies and limits`.

### Task 6: Server builder, listener, accept and handler dispatch

**Files:** Create `src/server/mod.rs`, `src/server/builder.rs`, `src/server/request.rs`, `src/server/response.rs`, `src/server/handler.rs`, `src/server/conn.rs`; modify `src/lib.rs`, `src/runtime/reactor.rs`.

**Interfaces:** Produces `Server`, `ServerBuilder`, `ServerHandle::{shutdown,shutdown_with_deadline,stats,runtime,local_addrs}`, `Handler::handle(&mut Request, &mut Response)`, `Request`/`Response` public methods per design §6.3; private `Server` attaches to a Runtime and runs listeners.

- [ ] **Step 1: Tests: bind port 0 and loopback GET; handler returns status/headers/body; streaming request echo; panics → 500 before commit/reset after; server shutdown drains.**
- [ ] **Step 2: Run focused test FAIL.**
- [ ] **Step 3: Implement builder/listener/accept/connection dispatch on M1 reactors; worker dispatch; Request/Response wrappers.**
- [ ] **Step 4: Run tests + clippy/fmt.**
- [ ] **Step 5: Commit** `feat(server): HTTP/1.1 server dispatch`.

### Task 7: Keep-alive, pipelining and h1 ordering

**Files:** Modify `src/server/conn.rs`, `src/server/mod.rs`, `src/http/h1/*`.

**Interfaces:** Produces per-connection request queue, in-order response commitment, keep-alive rules, max pipelined requests; h1 parked until response end.

- [ ] **Step 1: Tests: two pipelined GETs answered in order; bodyless keep-alive; close on `Connection: close`; pipelined request after a deferred response waits; smuggling corpus stays rejected.**
- [ ] **Step 2: Focused FAIL.**
- [ ] **Step 3: Implement queue/order/keep-alive; one response owner at a time.**
- [ ] **Step 4: Run tests + clippy/fmt.**
- [ ] **Step 5: Commit** `feat(h1): keep-alive and pipelining`.

### Task 8: Deferred completion and terminal fallback

**Files:** Create `src/server/deferred.rs`; modify `src/server/request.rs`, `src/server/response.rs`, `src/runtime/worker.rs`.

**Interfaces:** Produces `DeferredRequest`, `DeferredResponse`, `DeferredFailure`, `DeferredFailureReason`, `Request::{defer,defer_with_fallback}` exactly as design §6.3; detached table; worker fallback job; 504/503/500 defaults.

- [ ] **Step 1: Tests: defer succeeds atomically; Overloaded/InvalidRequest leave sync path; detached body streaming; timeout 504; shutdown 503; drop 500; custom hook mutates response; hook Err/panic default only before commit; committed/peer-gone reset; h1 order; no double commit.**
- [ ] **Step 2: Focused FAIL.**
- [ ] **Step 3: Implement handles, detached table, deadlines/cancel, fallback job and h1 close/drain.**
- [ ] **Step 4: Run tests + clippy/fmt.**
- [ ] **Step 5: Commit** `feat(server): deferred completion and fallback`.

### Task 9: M2a integration, smuggling/fuzz and server G1

**Files:** Create `tests/http1_server.rs`, `fuzz/` corpus, `benches/`; modify CI and `ServerBuilder`/limits as needed.

**Interfaces:** Public server API stable; no TLS/h2/h3.

- [ ] **Step 1: Add integration tests: external TCP client, loopback, keep-alive/pipelining, chunked request/response, trailers, deferred order, graceful shutdown; smuggling corpus (CL/TE, duplicate CL, obs-fold, bad chunking, absolute Host).**
- [ ] **Step 2: Add fuzz targets/seeds for head parser; run a bounded corpus in CI.**
- [ ] **Step 3: Add server G1 benchmark on the fixed host; record throughput/p99/syscalls/allocs; gate ≥0.95× reference.**
- [ ] **Step 4: Cross-platform CI (Linux/macOS/Windows/FreeBSD/MSRV); fix platform failures.**
- [ ] **Step 5: Commit** `test(m2a): server integration, fuzz and G1`.
