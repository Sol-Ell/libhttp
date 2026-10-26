# libhttp — Design (umbrella)

**Status:** umbrella. Contracts live in the subsystem documents listed below; this document holds intent, decisions, architecture, milestones and gates, and nothing that a subsystem document already states.
**Date:** 2026-10-26.
**Companion:** `2026-10-26-libhttp-requirements.md` (the brief).

**Why it is split.** The previous single-document design grew to 700 lines covering seven subsystems, and four verification passes never made it implementable — not because any one finding was hard, but because a document that size cannot be verified exhaustively, and every attempt to patch it moved the hole rather than closing it. Each subsystem is now small enough to be read whole and verified to exhaustion.

---

## 1. Intent

libhttp is a Rust HTTP library — not a framework, not a runtime. It exists because the available stacks force an abstraction and hide the copies and wakeups that decide performance.

The bet: **explicitness and speed are the same thing here**. If every copy, allocation, wakeup and syscall has a name and an owner, the fast path stops being a special case.

- The application owns its threads and its event loop; libhttp owns sockets, buffers, timers and protocol state.
- The public I/O contract is readiness; completion-based I/O is a submission path, never a readiness substitute.
- Protocol engines are sans-I/O byte machines behind one event vocabulary, so an h1↔h3 relay is the same code as h2↔h2.

---

## 2. Goals, non-goals, anti-goals

**Goals.** HTTP/1.1, HTTP/2, HTTP/3, client and server, over TLS, plus a reverse proxy built from public pieces only. Synchronous application code. No `Future`, `Waker`, executor or runtime anywhere. No thread per connection, no blocking socket I/O on an I/O thread, no threads spawned by the core. Identical public API on Linux, macOS/BSD and Windows.

**Non-goals.** A web framework. Tokio or any async-runtime integration. HTTP/2 push and priority. HTTP/3 migration, 0-RTT, Alt-Svc, Happy Eyeballs. A general-purpose I/O library.

**Anti-goals.** A fast path that hides work. Two APIs for the same thing. Platform types in a signature. A number bought with correctness. A proxy that needs a private module.

---

## 3. Constraints

- No `async`/`.await`, no `Future`, `Waker`, or hidden executor — not for TLS, not for QUIC, not internally.
- No blocking socket I/O on an I/O thread; no thread per connection; no unbounded queue.
- Linux, macOS/BSD and Windows first-class; CI runs the functional suite on all three; numbers are per platform.
- Typed protocol model: `Method`, `StatusCode`, `Version`, typed header names and values, parsed targets. No `String`/`HashMap<String, String>` on the hot path.
- No `bytes`-style buffer implementation, no `httparse`, no `hyper`, `h2`, `h3`, `tokio` in the product.
- OS crates (`libc`, `windows-sys`, `io-uring`) allowed. rustls for TLS. QUIC transport delegated; HTTP/3 framing and QPACK ours.

---

## 4. Decisions and what they beat

| Decision | Chosen | Rejected, and why |
|---|---|---|
| Thread ownership | Application owns threads and the loop; library owns dispatch | A library-owned reactor is a hidden executor with extra steps |
| Public boundary | Packets and semantic events | Exchanges hide the flow-control, reset and half-close mechanics the proxy must drive |
| I/O contract | Level-triggered readiness | Completion-shaped: unrepresentable on kqueue without a helper thread and an operation lifecycle the caller manages |
| Completions | Submission only (datagrams, file bodies) | Completion-backed `send` cannot report writability |
| Credit | Consumption-based, one public body handle | Read-time credit lets a slow consumer pull the peer's window open |
| QPACK | Format-complete, dynamic table at M5 | Eviction and blocked streams are a correctness risk with a measurable, deferrable payoff |

---

## 5. Architecture

```
  application        threads, pump loop, handlers, the proxy
  ─────────────────────────────────────────────────────────────
  convenience        Server / Client / Workers            thin, feature-gated
  ─────────────────────────────────────────────────────────────
  connection         Conn: TLS + one protocol engine
  ─────────────────────────────────────────────────────────────
  protocol           h1 / h2 / h3 engines                  sans-I/O, shared events
  ─────────────────────────────────────────────────────────────
  transport          Driver: sockets, readiness, timers, datagrams
  ─────────────────────────────────────────────────────────────
  platform           epoll · kqueue · AFD/IOCP   (+ io_uring submission, phase 2)
```

Dependencies point down only. The transport does not know what HTTP is; protocol engines do not know what a socket is.

### Subsystem map

| Document | Owns | Status |
|---|---|---|
| `2026-10-26-libhttp-transport-design.md` | sockets, readiness, interest, timers, ids, datagrams, chunk pool, credit sinks, close/half-close | written, for verification |
| `libhttp-conn-design` | `Conn`: `feed`/`feed_eof`/`poll`/`output`/`finish`/`Drained`, protocol selection, `tick`/`arm` | pending |
| `libhttp-tls-design` | rustls integration, ALPN, `close_notify` | pending |
| `libhttp-h1-design` | parser, serializer, framing, smuggling, pipelines, tunnels | pending |
| `libhttp-h2-design` | frames, HPACK, flow control, streams, settings | pending |
| `libhttp-h3-design` | QUIC boundary, framing, QPACK, endpoint demux | pending |
| `libhttp-dispatch-design` | workers, queues, caps, cancellation | pending |
| `libhttp-proxy-design` | the relay, and the proof the public API is sufficient | pending |

A subsystem document may not restate another's contract; it references it. If two documents need to agree on a type, the lower one owns it.

---

## 6. Milestones and gates

**M1 — transport and HTTP/1.1.** Driver on three platforms, chunks and pool, the h1 engine, `Conn`, h1↔h1 proxy, parser fuzz targets.

**M2 — TLS and HTTP/2.** rustls in `Conn`; h2 frames, HPACK, flow control, credit; h2 client and server; proxy h1↔h2 and h2↔h2.

**M3 — HTTP/3.** quinn-proto behind the datagram edge, our framing and QPACK (capacity zero, literal-only encoder), h3 client and server, the remaining relay pairs, interop against an independent implementation.

**M4 — accelerators.** io_uring submission batching, `SEND_ZC`, splice, `sendfile`, `TransmitFile`, batched UDP. Each lands behind its own gate; a path that does not beat the portable path on the published harness is not the default.

**M5 — QPACK dynamic table.** Encoder eviction safety, decoder blocked streams, and a compression benchmark that has to show the win. Additive: same wire format, no public change.

### Gate policy

A milestone is an exit condition, not a date: the functional suite passes **and** the gate measures. Correctness blocks unconditionally. Performance blocks by default and can be waived in writing with the measured gap and a follow-up item; it is never closed quietly unmeasured.

Gates 1 (M1) and 2 (M2) compare against hyper on the same host, recording the CPU, kernel, rustc and hyper versions with the numbers, over four endpoints — a trivial GET, a pipelined GET pair, a 1 MiB body through the proxy, and a streaming upload through a worker — plus one combined load of 10k idle keep-alives with a single hot connection. Metrics: throughput, p50, p99, syscalls, allocations, copies.

Pass, per platform: throughput ≥ 95% of hyper, p99 ≤ 110%, copies and allocations no worse, all six numbers published with the harness.

Gate 3 (M3) is correctness-gated: interop against an independent implementation. There is no hyper h3, so its numbers are published, not compared to a hyper margin. Gate 4 is per accelerator, against the portable path. Gate 5 is the compression benchmark.

### Memory budget the gates report

Per-state targets live where the state is owned: the transport states the connection and pool rows (transport §7), `h2` will own the HPACK tables, `tls` the buffer limits, `h3` the transport state. The umbrella states only the totals the gate publishes, because a blended figure hides the largest term:

- 10k connections, TLS off: ~10 MiB (h1), ~120 MiB (h2), ~320 MiB (h3).
- 10k connections, TLS on: add the configured TLS term (48 KiB each) to whichever applies.
- Chunk pool: 64 MiB per shard, so 512 MiB at 8 shards — larger than either total, and reported separately.

Each row is a target its owning document states and the gate reports; none is a claim about a measurement that has not happened.

---

## 7. Crate and dependencies

One crate, `libhttp`. Edition 2024, MSRV 1.85.

| Feature | Adds | Why optional |
|---|---|---|
| `tls` | rustls, certificate config, `close_notify` | Some deployments terminate TLS elsewhere |
| `roots` | `webpki-roots` for the client | A default trust store is a policy choice |
| `h3` | quinn-proto, QPACK, the datagram edge | Largest dependency; h1/h2 users should not pay for it |
| `proxy` | the relay | Leaf functionality, public API only |
| `threads` | `Server`, `Client`, `Workers`, `WakeHandle` spawning | Keeps "the core spawns no threads" true by construction |

| Dependency | Reason |
|---|---|
| `rustls` | Sans-I/O TLS; no runtime |
| `ring` | rustls' crypto provider (§8) |
| `webpki-roots` | Client roots, opt-in |
| `quinn-proto` 0.11 | Sans-I/O QUIC transport; pure Rust, no runtime, MSRV 1.85 |
| `libc` | Unix syscalls |
| `windows-sys` | IOCP, AFD ioctls, socket options |
| `io-uring` | Phase 2 Linux accelerator only |

rustls and quinn-proto are configured deliberately because their defaults contradict this design: rustls defaults to aws-lc-rs, and quinn's default features pull both providers plus `rustls-platform-verifier` (ambient OS roots). We pin `ring`, disable quinn's defaults, and make roots explicit.

Dev-only: hyper (the baseline), an independent h2/h3 implementation (interop), a fuzz harness. Rejected QUIC transports: quiche (BoringSSL + cmake), s2n-quic (mandatory tokio), neqo (NSS), gm-quic (async-bound).

---

## 8. Open decision

**The C toolchain.** `ring` compiles C and assembly. The brief allows a C toolchain only "unless the QUIC decision requires it" — and it does: the transport must be sans-I/O, rustls is the sans-I/O TLS, and rustls has no mature pure-Rust provider. So this is covered by the brief's carve-out rather than a deviation from it, provided the README states the build requirement. Recommendation: accept and record it. If the carve-out is judged not to cover it, the TLS choice has to change.

This is the only open decision.

---

## 9. Risks

**AFD.** The Windows backend depends on an internal NT interface. A startup self-test verifies semantics before use and selects the `WSAPoll`+`ConnectEx` fallback on any mismatch, so an AFD that is unavailable or wrong **at startup** costs performance, not correctness. The residual risk the fallback does not cover is a semantic change **after** startup: nothing detects that, and it would be a correctness failure. The Windows gate numbers are scoped to AFD-present for that reason, and §17's self-test is the only mitigation.

**QPACK.** The dynamic table is deferred to M5 so that a compression bug cannot be confused with an interop bug between two QUIC stacks.

---

## 10. Verification

- Fuzz targets: h1 parser, h2 frame/HPACK decoder, h3/QPACK decoder, all reachable from a byte slice with no I/O.
- Conformance: an h1 smuggling corpus; h2 and h3 interop against an independent implementation, as client and as server.
- Portability: the functional suite on Linux, macOS and Windows on every change; numbers per platform, never extrapolated.
- The harness is published with the numbers, including syscalls, allocations and copies.

---

## Appendix A — Where each brief §10 question is answered

| # | Question | Answer |
|---|---|---|
| 1 | I/O model across platforms | Transport §4.1, §6 |
| 2 | Arming, backpressure, close, drain | Transport §4.2, §4.4, §4.6; graceful drain is `conn` |
| 3 | Buffer ownership and budget | Transport §5, §7 |
| 4 | Worker model | `dispatch` |
| 5 | Fast-path scope | Transport §6.3, §6 milestones M4 |
| 6 | QUIC backend and budget | `h3` |
| 7 | TLS integration and order | `tls`; M2 |
| 8 | File bodies | M4; **no contract written yet** — file bodies are unsupported until one exists |
| 9 | Half-close and tunnels | Transport §4.5; `proxy` |
| 10 | Socket ownership | Transport §3, §4.8 |
| 11 | Performance gate | §6 above |
| 12 | Crate, features, MSRV | §7 above |
