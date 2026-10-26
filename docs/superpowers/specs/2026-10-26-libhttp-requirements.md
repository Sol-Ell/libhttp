# libhttp — Requirements and Context for a Clean-Room Design

**Status:** handoff brief. This document *supersedes* design revisions 1–4 for the purpose of a new design.
**Date:** 2026-10-26.
**How to use it:** start a new session, read only this document, and design the library from
zero. Do not read the archived design or its git history; they contain accumulated
assumptions that this brief exists to remove. Use the brainstorming process first, then an
approaches discussion, then a new design and plan.

## 1. What is being built

A Rust HTTP library — not a framework. It must be fast, synchronous, explicit about cost,
and usable as a foundation rather than a boxed-in stack.

The library provides HTTP/1.1, HTTP/2, and HTTP/3 server and client functionality, TLS,
and a reverse proxy. It targets Linux, macOS/BSD, and Windows from the start. Platform
fast paths are welcome, but the public API and correctness must not depend on them.

The user's core complaint about existing Rust HTTP stacks is not that they use
abstractions. It is that they force abstractions, hide copies/allocations/wakeups, assume
an async runtime, and make performance impossible to reason about. `async`/`.await` is
rejected. Blocking socket I/O and thread-per-connection are rejected. A synchronous
handler API is required.

## 2. Non-negotiable requirements

- **No async runtime, no `Future`, no `Waker`, no hidden executor.** Not for TLS, not for
  QUIC, not internally.
- **No blocking socket I/O on an I/O thread.** A loop may sleep waiting for events; a
  worker may block on its own database or on a queue; no thread blocks on a socket read or
  write.
- **No thread per connection.**
- **Synchronous handlers.** Handlers are ordinary functions on a worker pool; the library
  must not let a blocking handler stall I/O or other connections. Handler execution is the
  application's responsibility; the library provides bounded queues, deadlines, and
  cancellation.
- **HTTP/1.1, HTTP/2, HTTP/3, server and client**, plus TLS and a reverse proxy.
- **Linux, macOS/BSD, Windows are first-class.** Public API is identical across them.
- **Low-level API is public.** A reverse proxy with streaming bodies, CONNECT/half-close,
  flow control, and trailers must be expressible without private internals.
- **Typed protocol model.** `Method`, `StatusCode`, `Version`, typed header names/values,
  parsed targets; no `String`/`HashMap<String, String>` on the hot path; no
  `bytes`-style buffer implementation.
- **Bounded memory and visible costs.** Every copy, allocation, wakeup, and syscall should
  be documented and measurable.
- **Correctness first.** Malformed input never panics; request-smuggling defenses; fuzzed
  parsers; spec conformance.

## 3. Portability requirement (new emphasis)

The library must be portable. Platform-specific acceleration is optional and must not
change the public API.

- Identify capabilities at runtime (io_uring availability, provided buffers, zero-copy
  send, sendfile/splice, IOCP features, kqueue features).
- Every fast path needs a correct fallback on every supported platform.
- Public types must not expose Linux-only concepts (`fd`, `epoll`, `io_uring`, `IoSlice`
  quirks) as the only way to use the library.
- A platform-specific feature that is absent must degrade performance, not correctness or
  API shape.
- CI must build and run functional tests on all supported platforms; performance numbers
  are published per platform, not extrapolated.
- Avoid designing the core around a single OS primitive. A completion-shaped or
  readiness-shaped API may be chosen, but the other platform backends must fit it without
  lying about semantics.

## 4. Performance goals

"Better" means faster where performance is the criterion, but correctness is not tradable.

- 10k idle connections per reactor thread with measured memory, no thread per connection.
- No blocking socket calls; no thread-per-connection; no unbounded queues.
- Zero-copy where the kernel permits it: body forwarding, large writes, file bodies where
  the platform supports it.
- Avoid per-request heap allocation after warm-up where achievable; state exactly where
  allocations and refcounts remain.
- Avoid per-chunk cross-thread wakeups; batch where possible.
- Measure against hyper on a fixed host with recorded CPU, kernel, rustc, and hyper
  version: throughput, p50, p99, syscalls, allocations, copies. Publish the harness.
- Fast paths are benchmark-gated. A fast path that does not beat the portable path on the
  published benchmark is not the default.
- Do not claim "zero copy" or "no atomics" unless the common case actually achieves it;
  locate the copy/atomic precisely.

## 5. Functional scope

**HTTP/1.1**
- Request/response parsing and serialization; chunked; trailers; `100-continue`.
- Keep-alive, pipelining with response ordering; HTTP/1.0 behavior.
- Framing rules and request-smuggling defenses: CL/TE conflicts, duplicate CL, obs-fold,
  whitespace before colon, bare CR/LF, chunk-size oddities, undrained bodies.
- CONNECT and `101` switch to an opaque tunnel with half-close support.
- Client request bodies: buffered and streaming; responses streaming.

**HTTP/2**
- Frames, stream states, SETTINGS, GOAWAY, RST_STREAM, flow control, HPACK.
- Pseudo-headers, trailers, content-length checking, forbidden connection fields.
- h2c prior knowledge; no `101`.
- Plain CONNECT; extended CONNECT (RFC 8441) is out of scope for v1.
- Server push and legacy priority omitted.
- Worker dispatch per stream; a slow handler or slow consumer must not stall the
  connection's other streams.

**HTTP/3**
- QUIC transport delegated to a sans-io implementation (quinn-proto was the previous
  choice; re-evaluate quiche/others if justified). Do not hand-roll QUIC in v1.
- HTTP/3 framing, QPACK, control streams, request streams, settings/GOAWAY are ours.
- 0-RTT, connection migration, Alt-Svc/Happy Eyeballs are later decisions, not core.
- State the real copy/syscall budget at the quinn boundary; do not claim zero-copy if the
  boundary copies.
- A QUIC datagram surface that supports batching and per-datagram metadata (local address,
  ECN) is preferable; if v1 is one datagram per operation, state and measure it.

**TLS**
- rustls (sans-io) for HTTP/1.1 and HTTP/2; ALPN; certificate configuration;
  `close_notify`; no handshake blocking the event loop.
- QUIC uses rustls through the QUIC backend.
- TLS must not force a second TLS stack or a C toolchain unless the QUIC decision requires
  it.

**Proxy**
- Event-driven relay using only public low-level types.
- h1↔h1, h1↔h2, h2↔h2 in v1; h3 relay follows h3.
- Streaming bodies, trailers, flow control, RST/GOAWAY propagation, hop-by-hop field
  stripping, CONNECT and `101` tunnels with half-close.
- If the proxy needs a private API, the design failed.

## 6. API expectations

- Low-level public types: a driver/reactor/session layer, I/O operations and completions,
  connection engines, stream/exchange handles, buffers/pools, and errors.
- High-level `Server`, `Client`, `Handler`; sync handlers; streaming request and response.
- No forced `Body` trait, no `Service` tower, no runtime, no hidden global state.
- Traits, generics, and occasional trait objects are acceptable when they buy something.
  They are not acceptable as per-byte or per-field dispatch or as mandatory user
  ceremony.
- Typed headers: concrete `HeaderName`, `HeaderValue`, `Header`, ordered field storage with
  case-insensitive lookup; no map that loses order/duplicates.
- Errors typed; no `Box<dyn Error>` on the hot path.
- Public API must not leak the chosen platform I/O model (completion vs readiness) as the
  only usable path, unless that model is uniform and documented across all platforms.

## 7. Dependency and abstraction philosophy

- Dependencies must prove measured value in performance, portability, or correctness.
- `bytes` implementations are distrusted (its trait definitions are acceptable); do not
  build the core buffer on `Bytes`/`BytesMut`.
- `libringbuf` is not mandatory; use it only if it is actually the fastest shape for the
  chosen design.
- `httparse` is distrusted; implement the HTTP/1 parser or use a clearly better approach.
- Do not use an HTTP framework, `hyper`, `h2`, or `tokio` in the product.
- `mio` is not required; native backends or a minimal portability layer are open choices.
- OS crates (`libc`, `windows-sys`, `io-uring`) are acceptable.
- QUIC transport may be delegated; HTTP/3 framing and QPACK must be ours.
- rustls for TLS.
- No dependency may force a different threading, buffering, or lifetime model on the
  public API.
- Abstractions that pay for HTTP/3 are in scope now; HTTP/3 is not a future protocol.

## 8. Lessons from revisions 1–4 (pitfalls to avoid, not code to copy)

These are the concrete failure modes the new design must solve. They are not architecture
prescriptions.

**I/O and completion**
- Do not build a fast path on completions without a completion-loss story. Require
  no-drop semantics or treat CQ overflow as fatal; size queues from a hard in-flight cap.
- Zero-copy send (`SEND_ZC`) is a two-completion protocol with partial sends, fallback,
  notifications after close, and iovec limits. Model it explicitly or leave it out of v1.
- One logical read per socket vs multishot is a contract, not an implementation detail.
  Define: who re-arms, how a connection pauses reads, what a terminal completion looks
  like, what happens to in-flight work on close, and when an operation id is reusable.
- A shared provided-buffer ring cannot enforce a per-connection budget by itself; define
  per-socket groups, a share, or single-shot fallback, and what `-ENOBUFS` does.
- Buffer accounting on close must not wait for user-held buffers, and the session object
  must not be the owner of the drain state. The reactor/timer/socket table must own
  lifetimes.
- Socket ownership needs a mutable, authoritative reactor table. A fixed `sockets()` slice
  at registration cannot handle accepted/connected sockets or ownership transfer.
- Half-close (`shutdown(SHUT_WR)`) is required for CONNECT/`101` tunnels and TLS
  `close_notify`; absence is a design bug, not a detail.
- Datagrams need batching and metadata if HTTP/3 is to meet a serious performance bar; if
  v1 is one datagram per op, say so and budget it.
- File bodies: workers cannot open/read files; opening on the event loop is blocking. Use
  preopened handles/caches, or an explicit off-loop file operation. On platforms without
  sendfile, the fallback needs a defined read path.
- Deadlines must have one owner. Do not race a worker timeout against a reactor close at
  the same instant. Timers belong in one heap/owner; stale timeout events are
  generation-checked.

**Buffers**
- A unique-owner fast path is good, but a read containing body plus the next request head
  forces a split in the common case. Locate the atomic/CAS honestly; consider a
  benchmarked tail-copy policy for small remainders.
- A move-only buffer cannot split one read among several HTTP/2/HTTP/3 streams without a
  copy or per-frame reads. Do not claim zero-copy in that case.
- Cross-thread buffer returns need batched wakeups; a per-buffer eventfd is not acceptable.
- `Buf`/`BufMut` must state `Send`/`Sync`/`Clone` behavior. Handles that are not `Sync`
  need a reason.

**Flow control and workers**
- A peer with a zero send window can park one worker per stream for the full write timeout.
  A shared pool then starves the server. Need a short credit-stall deadline, a per-
  connection cap on credit-parked workers, and/or `try_write`/flush semantics.
- A global buffered-DATA cap that simply stops connection credit makes one slow stream
  stall all streams. Use per-stream reservations summing to the cap, and reset a stream
  that cannot drain; always return connection credit for discarded DATA.
- A global worker queue is simple and balances blocking handlers; per-shard pools improve
  locality but can isolate a stuck shard. Make topology a stated choice, not an accident.
- Slow request bodies and slow consumers need explicit deadlines and cancellation. Do not
  let a handler's blocked read be an unbounded DoS vector.

**HTTP/2 and HTTP/3**
- One slow stream must not stall the connection; verify with flow-control queueing.
- QPACK has three control streams (control, encoder, decoder) and needs per-stream output.
  A single `WriteBufs` sink is not enough. Encoding must let the framing layer prepend
  lengths.
- A self-referential head API does not typecheck. Define an owned head (arena + spans) and
  a borrowed view reconstructed per call.
- Do not make the public API depend on a QUIC library's buffer types; hide the boundary
  copy and publish its cost.

**Process**
- Each revision piled assumptions on the previous one. Start from requirements, not from
  prior type names.
- Do not defer the public contracts (read arming, close, buffers, cancellation) to "what
  is left to the builder"; those are the design.
- A performance gate must be attached to a milestone that can actually run the endpoint
  and the fast path. Do not gate M3 on a fast path that lands at M4.

## 9. Decisions already made by the user

- Library, not framework.
- Sync API, no async runtime.
- No blocking socket I/O, no thread-per-connection.
- HTTP/1.1, HTTP/2, HTTP/3; server and client; TLS; proxy.
- All major platforms; platform-specific acceleration must not break the API.
- Low-level public API; proxy is the acid test.
- Dependencies must prove measured value.
- `bytes` implementations distrusted; `libringbuf` optional; `httparse` distrusted.
- QUIC transport may be delegated; HTTP/3/QPACK ours.
- The design must solve, not repeat, the pitfalls in §8.

## 10. Open questions the new design must answer

1. I/O model across platforms: completion-shaped, readiness-shaped, or a hybrid; how the
   public low-level API stays uniform without lying about semantics.
2. Read/accept arming and per-connection backpressure: one shot vs multishot; pause/cancel;
   terminal completion; operation-id lifetime; close/drain semantics.
3. Buffer ownership: unique/shared state, pooling, cross-thread returns, zero-copy split
   limits, and the exact copy/atomic budget.
4. Worker model: shared vs per-shard pools; how sync handlers block safely; credit-stall
   deadlines; cancellation; per-connection caps.
5. Fast-path scope: which platform-specific operations are v1 (multishot recv/provided
   buffers, zero-copy send, sendfile/splice, batched UDP) and which are phase 2.
6. QUIC backend choice and its real copy/syscall budget; h3 client discovery/0-RTT scope.
7. TLS integration and milestone order.
8. File-bodied responses: supported or not in v1; if yes, how files are opened/read off the
   reactor and how workers name them.
9. Half-close and tunnel semantics across all I/O models.
10. Socket ownership: authoritative tables, transfer on registration, close/drain, and
    listener ownership.
11. Performance gate: endpoint, milestone, host, and what may change if it misses.
12. Crate layout, features, MSRV, and dependency policy.

## 11. Suggested process for the new session

1. Read this brief only. Do not open the archived design.
2. Use the brainstorming skill to settle what the library is and is not; then lay out two
   or three architectural approaches and pick one.
3. Write a new design document with a different name. Do not reuse the old type names,
   module layout, or milestone order unless you independently choose them.
4. Use fresh subagents for alternatives or adversarial review if useful, but give them this
   brief, not the old design.
5. Run a builder check on the new design; resolve its questions in the document; get design
   approval; then write the plan.
6. Keep public contracts (arming, close, buffers, cancellation, half-close) in the design,
   not in a builder-discretion list.

## 12. What not to inherit

Do not inherit the old design's completion/readiness choice, `Op`/`Session`/`Completion`
types, `Buf`/`BufMut` design, credit-core shape, worker topology, milestone order, fast-path
scope, or namespace. They were attempts, not requirements. Reuse only the requirements,
user preferences, and the §8 failure lessons.

The goal is a design that is faster to reason about than the previous one, not a design
that preserves its decisions.
