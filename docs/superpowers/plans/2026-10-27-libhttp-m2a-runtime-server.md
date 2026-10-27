# libhttp M2a — Runtime and HTTP/1.1 Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the public shared `Runtime`, worker/admission layer and a real HTTP/1.1 server on the M1 transport spine. No client in this plan.

**Architecture:** M1 reactors/connections/pools remain private. A public `Runtime` owns reactors, workers and admission; `Server` attaches listeners and dispatches exchanges to a bounded worker pool. HTTP/1.1 heads are sans-I/O; bodies stream through bounded `BodyRing` queues. Deferred completion uses the approved terminal fallback. The M1 Unix/IOCP reactors gain a persistent command/listener loop instead of the single-use test loop.

**Tech Stack:** Rust 2024, MSRV 1.85, no async runtime, no `tokio`/`hyper` in product. Dev-only benchmark tools may depend on `hyper`/`tokio`; they are not library dependencies.

**Spec:** `docs/superpowers/specs/2026-10-26-libhttp-sync-http-design.md`, runtime/execution/protocols designs, M1 plan and `src/rt/`.

## Global Constraints

- Edition 2024; MSRV 1.85; one published crate `libhttp`; no product async/`Future`/`Waker`/executor.
- No blocking socket I/O on reactors; handlers run only on workers; no thread-per-connection.
- Server limits: h1 head 16 KiB; trailers 8 KiB; max body from `Limits`; 64 KiB response header; connection/exchange/deferred caps from `Limits`; 256 MiB active + 64 MiB pinned buffers.
- M2a is server-only: no `Runtime::client`, resolver/client-callback knobs, `send`/`fetch`, TLS, h2 or h3.
- Public signatures/semantics come from design §6.1–6.5; no public `unsafe`.
- `Request::defer_with_fallback` uses `FnOnce(&mut Response, &DeferredFailure) -> Result<(), Error>`; no `FallbackResponse`, no separate observer.

## Review Focus

1. A slow/malicious h1 peer must not make a reactor or worker allocate unbounded memory.
2. Pipelining preserves request order and never mixes bodies/responses.
3. Deferred completion never double-commits or silently truncates; fallback follows 504/503/500.
4. Shutdown/cancellation wakes blocked body operations and never touches freed rings.
5. Typed/raw header disagreement must not produce smuggling or a handler-visible mismatch.

## Dependency Order

`1 primitives/config → 2 Runtime+reactor loop → 3 workers/admission → 4 h1 head → 5 BodyRing → 6a server/listener/test client → 6b Handler/Request/Response/framing → 7 keep-alive/pipelining/deadlines → 8a defer atomic → 8b fallback/terminal → 9 integration/fuzz/server-G1`.

No task may depend on the M2b client. Task 5 uses a synthetic `BodyRing` producer/consumer; Task 6b wires it to sockets.

---

### Task 1: Public primitives, config, errors and standard headers

**Files:** Create `src/http/{mod,method,status,version,header,target,body}.rs`, `src/{error,limits,timeouts,stats,cancel}.rs`; modify `src/lib.rs`.

**Interfaces:** Produces public `Method`, `StatusCode`, `Version`, `HeaderName`, `HeaderValue`, `HeaderRef`, `HeaderMap`, `Header`, `HeaderError`, `Target`, `Uri`, `BodyChunk`, `IntoResponseBody`, `Error`, `TimeoutKind`, `ProtocolError`, `ProtocolErrorKind`, `TlsError`, `Limits`, `Timeouts`, `Stats`, `CancelToken`; root/module exports exactly per design §6.1/§6.4/§6.5. Add standard typed headers: `Host`, `ContentLength`, `TransferEncoding`, `Connection`, `Date`, `ContentType`, `ContentEncoding`, `Trailer`, `Expect`, `Server`, `RetryAfter`, `Location`, `SetCookie`.

- [ ] **Step 1: Tests**

```rust
assert!(Method::GET.as_str() == "GET");
assert!(Method::from_bytes(b"BAD METHOD").is_err());
assert!(StatusCode::from_u16(99).is_err() && StatusCode::from_u16(600).is_err());
assert!(matches!(Version::from_bytes(b"HTTP/1.1"), Ok(Version::Http11)));
assert!(Version::from_bytes(b"HTTP/2").is_err()); // M2a h1 only
assert!(HeaderValue::try_from("a\r\nb").is_err());
assert!(HeaderValue::try_from("a\tb").is_err()); // design rejects CTLs
// HeaderMap: set replaces, singleton append rejected, Set-Cookie multiple allowed.
// Limits defaults: max_head_bytes == 16*1024, max_trailer_bytes == 8*1024,
// max_connections == 10_000, max_deferred_exchanges == 4_096.
```

- [ ] **Step 2: `cargo test --lib http::`** — FAIL unresolved types.
- [ ] **Step 3: Implement validation and config; header names lowercase tokens; raw/typed share one map; no arena storage yet.**
- [ ] **Step 4: `cargo test --lib http:: && cargo clippy --all-targets -- -D warnings`** — PASS.
- [ ] **Step 5: Commit** `feat(http): public primitives, limits and standard headers`.

### Task 2: Runtime core, persistent reactor loop and server attachment

**Files:** Create `src/runtime/{mod,reactor,command,stats}.rs`; modify `src/rt/reactor/{mod,unix,iocp}.rs`, `src/rt/mod.rs`, `src/lib.rs`.

**Interfaces:** Produces `Runtime::{builder,server,handle}`, `RuntimeBuilder::{reactors,workers,build}` (no client knobs), `RuntimeHandle::{shutdown,shutdown_with_deadline,wait,stats}`, `RuntimeStats`; `Runtime::server()` returns a builder attached to the runtime (implemented in Task 6a); private runtime for `Server::builder()`. Reactor command enum: `AddListener`, `Connect`, `Close`, `Shutdown`, `Drain`, with one persistent run loop and a wake; reactors are no longer single-use.

- [ ] **Step 1: Tests: two `Runtime`s can coexist; `reactors(0)` fails; `workers(0)` builds but server build fails `InvalidConfig`; `RuntimeHandle::shutdown` joins; a second listener can be added after startup; run loop continues after an unknown/closed command; no blocking socket call in reactor code.**
- [ ] **Step 2: Focused tests FAIL unresolved Runtime/command.**
- [ ] **Step 3: Implement runtime ownership, reactor threads, command queue, persistent loop, wake/stop; no HTTP.**
- [ ] **Step 4: Focused/full tests + clippy/fmt; no Windows runtime locally, cross-check.**
- [ ] **Step 5: Commit** `feat(runtime): shared runtime and persistent reactor commands`.

### Task 3: Worker pool, dispatch and admission

**Files:** Create `src/runtime/{worker,admission}.rs`; modify `src/runtime/mod.rs`.

**Interfaces:** Produces `WorkerPool::{new,dispatch,shutdown}`, bounded queue, `Admission::{try_acquire,release,inflight,queued}`, per-connection exchange cap, cancellation token for queued jobs. Handlers never run on reactors.

- [ ] **Step 1: Tests: job runs on worker thread; full queue returns `Overloaded`; shutdown wakes queued jobs; cancellation skips queued work; a job blocked past shutdown is not joined forever (worker owned by runtime).**
- [ ] **Step 2: FAIL unresolved WorkerPool.**
- [ ] **Step 3: Implement pool/admission with server limits; no client call limits in M2a.**
- [ ] **Step 4: Focused/full tests + clippy/fmt.**
- [ ] **Step 5: Commit** `feat(runtime): bounded server worker dispatch`.

### Task 4: Strict HTTP/1.1 head parser/serializer

**Files:** Create `src/http/h1/{mod,head,parse,encode}.rs`; modify `src/http/mod.rs`.

**Interfaces:** Produces `HeadBuf::{feed,parse_request_head,consumed,leftover}`, `H1Limits` (16 KiB head, max header count/bytes), `ReceivedHead<'a>` with method/target/version/headers, and `encode_response_head(&ResponseHead, &mut Vec<u8>, Version) -> Result<(), Error>`. Parser returns `ProtocolErrorKind`; server maps to 400/431. No response parser in M2a.

- [ ] **Step 1: Tests: valid GET; incremental feed split mid-line; absolute-form Host mismatch rejected; duplicate CL rejected; TE+CL rejected; non-final chunked rejected; obs-fold/NUL/bare CR rejected; 16 KiB head returns `HeadTooLarge`/431 mapping at server; response encoding for CL/chunked/Connection.**
- [ ] **Step 2: `cargo test --lib http::h1` — FAIL.**
- [ ] **Step 3: Implement strict h1 parser/serializer; no I/O; no h2 response parser.**
- [ ] **Step 4: Run tests + clippy/fmt.**
- [ ] **Step 5: Commit** `feat(h1): strict head parser and serializer`.

### Task 5: BodyRing, streaming bodies and limits

**Files:** Create `src/http/body.rs`, `src/runtime/body_ring.rs`; modify `src/http/mod.rs`, `src/limits.rs`.

**Interfaces:** Produces `BodyRing` (bounded producer/consumer with cancel), `BodyReader::{read,next_chunk,read_to_end,is_end}`, `BodyWriter::{write,write_all,flush,finish,finish_with_trailers}`, `BodyChunk::as_bytes`; `BodyRing` synthetic tests do not need sockets. Wiring to server is Task 6b.

- [ ] **Step 1: Tests with a synthetic producer/consumer: in-order bytes; zero-copy `next_chunk` accounting; `max_body_bytes` error; streamed request/response; trailers 8 KiB; blocked read/write wakes on cancel; retained chunks count pinned (64 MiB cap).**
- [ ] **Step 2: FAIL unresolved BodyRing/body types.**
- [ ] **Step 3: Implement rings over M1 blocks/pool; body operations block only app/worker threads; no reactor I/O.**
- [ ] **Step 4: Run tests + clippy/fmt.**
- [ ] **Step 5: Commit** `feat(http): bounded streaming body ring`.

### Task 6a: Server builder, listener, accept, dispatch and TCP test client

**Files:** Create `src/server/{mod,builder,listener}.rs`, `src/server/test_client.rs`; modify `src/runtime/mod.rs`, `src/lib.rs`.

**Interfaces:** Produces `Server`, `ServerBuilder::{bind,limits,timeouts,on_error,register_header,build}`, `ServerHandle::{shutdown,shutdown_with_deadline,stats,runtime,local_addrs}`, `Server::run(handler) -> Result<(), Error>`, private runtime build; a test-only raw `TcpStream` client in `src/server/test_client.rs`. Handler/Request/Response public surface is Task 6b; 6a dispatches a minimal echo/inspect handler.

- [ ] **Step 1: Tests: bind port 0; handle local_addrs; raw client GET; keep-alive close; server shutdown; shared Runtime server; listener add/remove; no handler on reactor.**
- [ ] **Step 2: FAIL unresolved Server.**
- [ ] **Step 3: Implement builder/listener/accept/dispatch; use M1 reactor commands; minimal Request/Response placeholders wired in 6b.**
- [ ] **Step 4: Run tests + clippy/fmt.**
- [ ] **Step 5: Commit** `feat(server): listener, accept and dispatch`.

### Task 6b: Handler, Request, Response, h1 framing

**Files:** Create `src/server/{request,response,handler,framing}.rs`; modify `src/server/mod.rs`, `src/runtime/worker.rs`.

**Interfaces:** Produces public `Handler::{handle}` blanket closure, `Request`/`Response` methods per design §6.3, `BodyReader`/`BodyWriter` wiring, typed decode-on-worker (arena per head), status/headers/body framing, `Expect: 100-continue`/417, request drain rules, panic/error → 500/reset.

- [ ] **Step 1: Tests: GET/POST echo; typed header decode; raw/typed consistency; HEAD/204/304/1xx framing; chunked response; Expect 100; unread body drain; handler panic before/after commit; 400/413/431 paths.**
- [ ] **Step 2: FAIL unresolved Request/Response.**
- [ ] **Step 3: Implement Request/Response/handler; decode typed headers on worker; arena charged to limits.**
- [ ] **Step 4: Run tests + clippy/fmt.**
- [ ] **Step 5: Commit** `feat(server): request, response and h1 framing`.

### Task 7: Keep-alive, pipelining, deadlines and shutdown

**Files:** Modify `src/server/{listener,conn,framing}.rs`, `src/runtime/{command,admission}.rs`.

**Interfaces:** Produces per-connection request queue, in-order response commit, keep-alive/close, max pipelined requests, header/body/keep-alive/total/shutdown deadlines, accept caps, graceful drain. One response owner per connection at a time.

- [ ] **Step 1: Tests: two pipelined GETs in order; bodyless keep-alive; Connection close; pipelined request waits behind deferred response; header read timeout; body idle timeout; keep-alive timeout; shutdown grace/drain; accept cap `Overloaded`.**
- [ ] **Step 2: FAIL unresolved queue/deadlines.**
- [ ] **Step 3: Implement queue/order/deadlines; no smuggling regression.**
- [ ] **Step 4: Run tests + clippy/fmt.**
- [ ] **Step 5: Commit** `feat(h1): keep-alive, pipelining and deadlines`.

### Task 8a: Deferred completion atomicity

**Files:** Create `src/server/deferred.rs`; modify `src/server/{request,response}.rs`.

**Interfaces:** Produces `DeferredRequest`, `DeferredResponse`, `Request::{defer,defer_with_fallback}` handles, detached table, reserve/terminal validation, `Send` not `Sync`, blocked body reads/writes, timeout/cancel state. Fallback behavior is Task 8b.

- [ ] **Step 1: Tests: atomic success tombstones; Overloaded/InvalidRequest leave sync path; detached body streaming; timeout/cancel wakes; handles outlive run with `Canceled`; h1 parked; no fallback yet (defaults only).**
- [ ] **Step 2: FAIL unresolved DeferredRequest.**
- [ ] **Step 3: Implement handles/table/deadlines/cancel; no fallback hook yet.**
- [ ] **Step 4: Run tests + clippy/fmt.**
- [ ] **Step 5: Commit** `feat(server): atomic deferred completion`.

### Task 8b: Terminal fallback and direct emit

**Files:** Modify `src/server/deferred.rs`, `src/runtime/{worker,admission}.rs`, `src/server/framing.rs`.

**Interfaces:** Produces `DeferredFailure`/`DeferredFailureReason`, `FnOnce(&mut Response, &DeferredFailure)`, default 504/503/500, hook at most once, worker fallback job, reactor direct-default when no worker admits, h1 close/drain, h2 not in M2.

- [ ] **Step 1: Tests: timeout 504; shutdown 503; drop/panic 500; custom hook mutates response; hook Err/panic default only before commit; committed/peer-gone reset; no worker direct emit; h1 drain/order; no double commit.**
- [ ] **Step 2: FAIL unresolved fallback.**
- [ ] **Step 3: Implement fallback hook/table/job/direct emit; no separate observer.**
- [ ] **Step 4: Run tests + clippy/fmt.**
- [ ] **Step 5: Commit** `feat(server): deferred terminal fallback`.

### Task 9: Integration, smuggling corpus, fuzz and server G1

**Files:** Create `tests/http1_server.rs`, `fuzz/Cargo.toml`, `fuzz/fuzz_targets/h1_head.rs`, `fuzz/corpus/h1/*`, `bench/`; modify CI.

**Interfaces:** End-to-end server API; no client. M2a exit is **server G1**, not the final combined G1.

- [ ] **Step 1: Integration tests with raw TCP: keep-alive, pipelining, chunked/trailers, deferred order, shutdown; smuggling corpus: CL/TE, duplicate CL, obs-fold, bad chunks, absolute Host, bare LF/CR.**
- [ ] **Step 2: Fuzz target seeds; CI bounded corpus run; nightly job separate from stable.**
- [ ] **Step 3: Server G1 with external `wrk` (no product deps): 64/256/1024 connections; throughput ≥0.95× reference, p99 ≤1.25×, syscalls/request ≤1.1×, allocs/request ≤1 for benchmark headers; publish numbers.**
- [ ] **Step 4: CI Linux/macOS/Windows/FreeBSD/MSRV; fix platform failures.**
- [ ] **Step 5: Commit** `test(m2a): integration, fuzz and server G1`.

## Out of Scope

`Runtime::client`, `send`/`send_with`, `fetch`/`fetch_with`, resolver/client callbacks, TLS, HTTP/2, HTTP/3, response parsing for a client. These are M2b/M3+.

## Open Decisions

- M2a's server-only G1 is a milestone gate; the full G1 (client+server) is M2b. The roadmap is updated to state this.
- HeaderMap may start boxed; the parser task must preserve public semantics and add arena accounting when wiring heads.
- Standard headers are M2a; full `register_header` collision behavior is Task 6a.
