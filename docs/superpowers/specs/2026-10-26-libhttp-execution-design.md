# libhttp — Execution Design (Workers, Exchanges, Deadlines, Limits)

**Status:** part of the design for approval. **Date:** 2026-10-26.
**Parent:** [2026-10-26-libhttp-sync-http-design.md](2026-10-26-libhttp-sync-http-design.md).
**Companion:** [runtime design](2026-10-26-libhttp-runtime-design.md) (buffers, timers, transport).
This document binds the worker, queue, deadline, cancellation and limit contracts. It exists because
these are the contracts most easily lost between a reactor design and a planner.

---

## 1. Threads and ownership

- **Reactor threads** own sockets, connection state machines, timers, buffer pools, and the socket
  table. Only the owning reactor mutates a connection.
- **Worker threads** run handlers. They own nothing shared except a per-exchange queue, a cancel flag,
  and a parking reservation. They never touch a socket, a timer, or a socket-table entry.
- **Client connections** run on the same runtime reactors as server connections; the client pool
  and call queue are reactor-owned. `Client::send`/`ClientExchange::finish` block the calling thread
  (application thread or, if the app chooses, a handler worker) on the exchange; there is no client
  call object, callback executor or library worker per request.
- **Cross-thread state** is per exchange: an inbound ring (reactor → worker), an outbound ring
  (worker → reactor), a terminal-state slot, two condition variables, and a parking reservation.
  Worker output marks the exchange ready and wakes the reactor at most once per batch, not per chunk.
- No global state: no static pools, no global timer thread, no global logger, no global executor.
  Building two `Server`s or `Client`s creates independent threads and state.

---

## 2. Exchange

An exchange is one request/response pair:

- request head: typed, borrowing a refcounted head block (§ runtime 4.2);
- inbound body ring: block ranges, bounded by `max_request_body_buffer`;
- response head: typed until the reactor serializes it (so HPACK/QPACK encoding stays on the reactor);
- outbound body ring: block ranges carrying writer output and the trailer slot;
- terminal state: `Open`, `Finished`, `Cut` (peer/shutdown), `Failed(Error)`, under one mutex;
- parking reservation and deadline state, owned by the reactor.

A handler runs on one worker for the whole exchange. A queued exchange holds **only its head**; its
body is not read until a worker is assigned, so queue depth does not multiply body buffers.

A **deferred exchange** has been moved out of the worker by `Request::defer`: the reactor holds its
exchange state in a bounded detached table, and the `DeferredRequest`/`DeferredResponse` handles are
owned by an application thread. It is not an executing exchange, does not occupy a worker, and uses
the same inbound/outbound rings and flow control as a handler-held exchange. Completion or a body
write from the application thread marks it ready and wakes the reactor through the same coalesced
wake; there is no per-chunk eventfd.

---

## 3. Worker pool and admission

One shared worker pool with one bounded dispatch queue; no per-reactor shards. A blocking handler
cannot isolate a shard, and a stuck shard cannot exist. The cost is one queue mutex plus one futex
wake per dispatch, measured by the harness.

- `max_inflight_exchanges` (`None` = workers) bounds executing exchanges globally.
- `max_queued_exchanges` (`None` = workers × 256) bounds waiting heads.
- `max_inflight_exchanges_per_connection` (`None` = `max(1, workers / 2)`; HTTP/1.1 is implicitly
  1) bounds executing exchanges on one connection. With more than one worker, a 32-stream HTTP/2 peer
  cannot occupy the whole pool: once
  the cap is reached, further streams stay open and wait (up to the advertised
  `MAX_CONCURRENT_STREAMS`), and streams beyond the advertised limit are refused with
  `REFUSED_STREAM` (h2) or reset (h3). Existing streams continue.
- When the global queue is full: HTTP/1.1 stops reading new heads; HTTP/2/3 refuse new streams once
  the per-connection cap is reached; accepts pause at the connection cap. Existing exchanges finish.
  Nothing queues without bound.
- A handler that returns without reading the request body triggers the drain rule (§ protocols,
  HTTP/1.1) and never blocks a reactor.
- A detached exchange is tracked separately and does **not** count against `max_inflight_exchanges`.
  `max_deferred_exchanges` (default 4,096) bounds the detached table; when it is full,
  `Request::defer` returns `Overloaded` and the handler can take the synchronous path. Detached
  exchanges do count as open for the per-connection buffer share and arena charging (§6, §8).
  Application-thread body I/O does not take a worker parking reservation; it is bounded by the
  connection/global caps and by `read_stall`/`write_stall`/`deferred` and cannot starve workers.
- Typed header decoding runs on the worker when the exchange is dispatched; on the client it runs on
  the `Client::send`/`ClientExchange::finish` thread before `ClientResponse` is returned. A queued
  exchange carries only its raw head. A decode
  failure is a `400`/stream error before the handler runs. Typed values live in a per-head arena
  bounded by `max_header_arena_bytes`.

---

## 4. Deadlines: one owner

The reactor timer heap is the single owner of every connection/exchange deadline. Entries carry the
connection generation **and an exchange sequence number**, because an h1 keep-alive slot can host
exchange N+1 without a generation change; a stale per-exchange timer is discarded by checking both,
plus the recorded deadline. Worker waits are untimed: they wait on the exchange condition variable and
re-check terminal state under the lock. The reactor writes the terminal outcome (`Timeout(kind)` or
`Canceled`), wakes the worker, and the worker returns it. There is no worker-side safety timer and no
race at the deadline instant; a worker can never report a timeout for an exchange the reactor still
treats as live.

| Deadline | Default | Owner | Outcome |
|---|---|---|---|
| header read | 10 s | reactor | `Timeout(HeaderRead)`, `431`/close |
| body idle (no bytes) | 30 s | reactor | `Timeout(BodyIdle)`, reset/close |
| read stall (per blocking read) | 15 s | reactor | `Timeout(ReadStall)`, reset/close |
| write stall | 15 s | reactor | `Timeout(WriteStall)`, reset/close |
| keep-alive idle | 60 s | reactor | close |
| TLS handshake | 10 s | reactor | `Timeout(TlsHandshake)`, close |
| total request (optional) | off | reactor | `Timeout(Total)`, reset/close |
| deferred | 60 s from defer | reactor | `Timeout(Deferred)`, cancel the detached exchange; earlier `total_request` wins |
| connect (client) | 10 s | reactor | `Timeout(Connect)` |
| call queue (client) | 30 s | reactor | `Timeout(PoolQueue)` |
| graceful shutdown | `Timeouts::shutdown_grace`, 30 s | reactor | `Canceled`, then close |

`read_stall` is per blocking read call, not per body: a peer dripping one byte every 29 s cannot hold
a worker past the first 15 s read wait. A peer dripping faster than `read_stall` but slower than the
application's patience is bounded only by `total_request`, which a server handling untrusted uploads
should set. `body_idle` remains the connection-level inactivity timeout.

---

## 5. Parking: bounded workers, bounded stalls

A parked worker is a worker blocked in the library waiting for a body read or write capacity. Parking
is how a slow peer consumes the pool, so it is capped at three levels and only taken when the call
must actually wait.

- **Available work does not park.** A read with buffered data returns it immediately; a write with
  outbound capacity completes immediately. No parking reservation is taken for either. Only a read
  with an empty queue or a write with a full queue reserves a parking slot.
- **Global parked-worker ceiling.** `max_parked_workers` (`None` = `max(1, workers / 2)`) bounds
  simultaneously parked workers, reserving at least half the pool for runnable handlers. If no
  reservation is free, the call returns `Error::Overloaded` immediately and the exchange is destroyed
  (h2/h3 stream reset, h1 close). It never queues behind a full parking lot.
- **Per-connection total.** `max_parked_per_connection` (`None` = `max(1, max_parked_workers / 2)`,
  so 2 with 8 workers) caps all parked readers and writers on one connection. One connection cannot
  consume the global parking budget; when its total is reached, the call gets `Error::Overloaded` and
  its stream is reset (h1 closes).
- **Direction sub-caps.** `max_parked_writers_per_connection` (1) and
  `max_parked_readers_per_connection` (1) are ceilings within the per-connection total; they exist to
  keep a duplex exchange from being starved by one direction.
- HTTP/2/3 flow-control stalls are therefore either resolved by data/capacity arriving, capped by
  these reservations, or reset; HTTP/1.1 has one writer, so `write_stall` is its only stall bound.

Each parked read/write is also bounded by `read_stall`/`write_stall`, so parking is temporary even
without the caps. A handler doing pure computation does not park; it holds a worker until it returns
(§7). Application-thread body I/O on a deferred handle does not take these parking reservations: it
blocks that application thread, is bounded by the same stalls and the connection/global buffer caps,
and never consumes a worker. `max_deferred_exchanges` is the detached-work bound, not
`max_parked_workers`.

---

## 6. One slow stream must not stall the connection (HTTP/2 and HTTP/3)

**HTTP/2.** The advertised connection window `W` (`h2_initial_connection_window`, default 1 MiB) is
partitioned into per-stream reservations: `share = max(4 KiB, W / h2_max_concurrent_streams)`, with
`W ≥ share × h2_max_concurrent_streams` enforced at build (`Error::InvalidConfig`). A stream can
always consume its own share; unused shares can be borrowed temporarily.

- Inbound DATA is charged against the stream's effective buffer and its share. When a stream's worker
  is slow and its buffered bytes reach its share, the reactor withholds that stream's share of
  connection credit *and* its own stream credit, and nothing else. Other streams keep their shares
  and continue.
- If a stream is at or above its share with no worker read progress for `read_stall`, it is reset
  (`RST_STREAM(CANCEL)`), its buffers are released, and connection-level credit for the discarded DATA
  is returned immediately. The rule is reachable: a stalled stream is identified by "no progress for
  T", not by an impossible "over its own cap while the global cap is free" condition.
- Connection credit is returned when discarded DATA is released, so a reset stream never permanently
  consumes connection window.

**HTTP/3.** quinn owns `max_data`, the connection window and stream receive windows; there is no
library-advertised h3 connection window to partition, so the h2 formula does not apply. The rule is
pull-limiting plus reset: we pull stream bytes only when the worker's queue has room (so QUIC's
stream/connection credit is the backpressure), use `h3_max_concurrent_streams` for the stream cap,
and reset a stream that has been at its pull limit with no worker progress for `read_stall`. The
reset returns the QUIC credit; a slow stream cannot hold connection credit hostage.

**Limits.** With the defaults (32 streams, 8 MiB connection budget, 64 KiB per-direction ceilings),
the effective total per stream is `min(128 KiB, budget / open_streams)`, split by demand; 32 streams
fit their ceilings, so the connection budget is not the normal path. A user-raised stream count
causes the effective per-stream total to fall to `budget / open_streams`, with borrowing.

The connection is never globally stalled while one stream is slow: the slow stream is throttled to
its share, then reset, and the rest continue.

---

## 7. Cancellation, panic, shutdown

- **Cancellation sources:** peer disconnect, `RST_STREAM`/`GOAWAY`, server shutdown, expired deadline,
  or a write after the exchange is cut. The reactor sets the terminal state under the exchange lock,
  wakes all waiters, and discards later writes. Readers/writers return `Canceled` or the specific
  timeout; a handler doing computation sees `is_canceled()` and should return.
- **Handlers cannot be preempted.** A CPU-bound handler keeps its worker until it returns; the pool
  protects reactors and other connections, not itself. Admission and parking caps keep the pool from
  being consumed by I/O waits; they cannot reclaim a spinning thread. This is documented, not hidden.
- **Panic:** `catch_unwind` around the handler. If the exchange was not deferred, a pre-commit panic
  becomes a `500`, and a post-commit panic resets (h2/h3) or closes (h1). If `Request::defer` already
  succeeded, the panic only cancels the detached exchange; it never synthesises a response for state
  owned by `DeferredRequest`/`DeferredResponse`. The worker returns to the pool; poisoned exchange
  state is discarded. Application `Header::decode`/`encode` panics are caught at the library
  boundary: input becomes `400`/stream error, output `500`/reset.
- **Shutdown:** `ServerHandle::shutdown()` signals and returns; `Server::run` closes listeners, sends
  GOAWAY / `Connection: close`, stops dispatch, gives in-flight and deferred exchanges `Timeouts::shutdown_grace` (30 s by
  default; `shutdown_with_deadline` overrides it for that call), then sets terminal `Canceled` on
the remaining deferred handles, resets/closes what remains, and joins the
  reactor threads; it returns `Ok(())` unless a reactor/listener failed fatally. Normal handlers are
  drained within the grace period. A handler that ignores cancellation but eventually returns only
  delays teardown by its own runtime; a handler that never returns keeps its worker thread and
  prevents `run` from returning. That is the single unbounded wait in the design, it is caused by user
  code, and Rust cannot preempt it; tests must not install never-returning handlers.

---

## 8. Limits and reactions

Every bound has a reaction. This is the table the implementation and tests are checked against.

| Bound | Default | Reaction when hit |
|---|---|---|
| `max_head_bytes` (h1) | 16 KiB | h1: `431`, close |
| `h2_max_header_list_bytes` | 64 KiB incl. 32 B/field | h2: stream/connection error (`ENHANCE_YOUR_CALM`/COMPRESSION_ERROR) |
| `h3_max_header_list_bytes` | 64 KiB incl. 32 B/field | h3: stream/connection error |
| `h2_header_table_size` | 4 KiB | HPACK table-size update; oversize insertion is a connection error |
| `max_header_count` | 100 | h1: `431`, close; h2/h3: stream/connection error |
| `max_body_bytes` (request) | 8 MiB | next read `BodyTooLarge`; h1 `413`/close, h2/h3 `413` then reset |
| `max_request_body_buffer` | 64 KiB/exchange ceiling | stop reading that stream/socket; effective total = min(128 KiB, connection budget / open streams) |
| `max_response_body_buffer` | 64 KiB/exchange ceiling | writer blocks if a parking slot is available, otherwise `Overloaded`+reset |
| `max_connection_buffer_bytes` | 8 MiB (h2/h3), 256 KiB (h1) | reset stream over its share / close connection |
| `max_buffered_bytes` (server) | 256 MiB | reset top consumers to 90% low-water; then stop reading/accepting; no indefinite global stall |
| `max_connections` | 100,000 | pause accepts |
| `max_connections_per_reactor` | 50,000 | pause accepts on that reactor |
| `max_pending_accepts` | 1,024 | pause accepts until the handoff queue drains |
| `max_inflight_exchanges` | workers | queue, then pause reads/refuse streams |
| `max_inflight_exchanges_per_connection` | max(1, workers/2) (h1: 1) | additional streams wait; beyond the advertised limit `REFUSED_STREAM`/reset |
| `max_deferred_exchanges` | 4,096 | server-wide reservation; `Request::defer` returns `Overloaded`; synchronous path remains |
| `Timeouts::deferred` | 60 s from defer | detached exchange `Timeout(Deferred)`, resources freed |
| `max_queued_exchanges` | workers × 256 | pause reads / `REFUSED_STREAM` |
| `max_pipelined_requests` | 8 | stop reading h1 until a slot drains |
| `h2_max_concurrent_streams` | 32 | streams over the advertised limit are `REFUSED_STREAM` |
| `h3_max_concurrent_streams` | 32 | streams over the advertised limit are reset |
| `max_parked_workers` | max(1, workers/2) | `Overloaded`, reset/close |
| `max_parked_per_connection` | max(1, parked/2) | `Overloaded`, reset that stream |
| `max_parked_writers_per_connection` | 1 | `Overloaded`, reset that stream |
| `max_parked_readers_per_connection` | 1 | `Overloaded`, reset that stream |
| `max_trailer_bytes` | 8 KiB | h1: `431`/close; h2/h3: stream error |
| `max_chunk_extension_bytes` (h1) | 1 KiB | `400`, close |
| `max_header_arena_bytes` | 64 KiB per active head; charged to connection/global caps | `431`/stream error; `defer` `Overloaded` if the global reservation fails |
| `h2_max_continuation_frames` | 8 | connection error |
| `max_qpack_decoded_bytes` | 64 KiB | h3 stream/connection error |

Memory arithmetic: idle connections hold zero blocks (§ runtime 4.3); active connections hold one
read block plus bounded exchange queues, capped at 8 MiB each; the sum of outstanding blocks and
typed-header arenas is capped globally at 256 MiB with a 90% low-water reset; queued exchanges hold
heads only. `max_queued_exchanges` therefore does not multiply body buffers, and `max_connections`
does not imply a block per connection. Detached exchanges hold their head/body/response buffers and
arena under the same per-connection and global caps and count as open; `max_deferred_exchanges`
bounds their count, not their bytes.

## 9. Deferred completion

`Request::defer(&mut self, &mut Response) -> Result<(DeferredRequest, DeferredResponse), Error>` is
the only entry to the deferred path. It is atomic and non-blocking:

1. Reserve a detached slot from the server-wide `max_deferred_exchanges` counter (atomic CAS). Failure
   is `Overloaded`; neither side moves.
2. Under the exchange lock, verify the exchange is not terminal, the response head is not committed,
   no response body is set, and the request is not already deferred. A terminal exchange is
   `Canceled`; invalid state is `InvalidRequest`; neither side moves.
3. Move the request head/body and response state into `Send` handles, mark the exchange detached, and
   publish the handles into the owning reactor's detached table through a pre-reserved slot. The
   publication cannot block; a reactor shutdown releases the reservation and the handles observe
   `Canceled`.

On success the handler must return promptly; the borrowed `Request`/`Response` are tombstones whose
methods return `Closed`/`InvalidRequest`. Success transfers the exchange: a later handler return
value or panic can only cancel it, never commit the tombstone. The worker is freed; the reactor keeps
the exchange and wakes on application body writes/completion.

The deferred handles are `Send` (not `Sync`) and support the same typed/raw headers, streaming request
and response bodies, trailers, deadlines, cancellation and h1/h2/h3 behavior as the synchronous path.
A body read/write blocks the application thread on the same bounded rings and deadlines; it does not
take a worker parking reservation and never blocks a reactor or library worker.

- h1 parks that connection until completion (one deferred exchange per h1 connection); h2/h3 keep
  other streams running under normal flow control and count the detached exchange as open for buffer
  shares.
- `Timeouts::deferred` defaults to 60 s from the successful defer; an earlier `total_request` wins.
  Expiry is `Timeout(Deferred)`; disconnect and shutdown are `Canceled`.
- Shutdown unlinks deferred exchanges after the grace period and marks them terminal. Handles may
  outlive `Server::run`: they hold an `Arc` to the terminal shared state, so blocked reads/writes wake
  with `Canceled`, `send` returns `Canceled`, and dropping them frees the last reference. Rings and
  buffers are released at cancellation, not waiting for the handles.
- Dropping `DeferredResponse` without `send` sends `500` if the head is uncommitted and the connection
  permits it, otherwise resets/closes. Dropping `DeferredRequest` with an unread body applies the
  normal drain/reset rule and may cancel the deferred response.
- This is per-request opt-in, not automatic migration: a handler already blocked inside a synchronous
  call cannot be detached by the library.

---

