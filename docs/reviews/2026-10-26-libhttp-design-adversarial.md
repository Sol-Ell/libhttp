# Adversarial review — libhttp design (2026-10-26)

Severity legend: **Critical** = produces wrong results or makes a stated gate unmeetable; **High** = a guarantee the design declares settled is false and the failure is reachable without an application bug; **Medium** = a public contract gap the builder must invent, with a client-visible consequence.

The design is internally inconsistent in five load-bearing places, each one a contract it declares settled: level-triggered readiness has no way to express "stop reading", so the backpressure story spins; the credit token is per allocation while h2/h3 DATA is per stream, so the celebrated zero-copy cross-stream split mis-attributes credit; chunk lifetimes are not bounded by the connection, so §6.4's "the application's own call is the only thing in flight" is false in the proxy's own relay path; `finish`/`shutdown_write` cannot order `close_notify` before FIN because the object that owns the bytes is not the object that shuts the socket; and `wait()` is called once per event batch while the loop it introduces is O(all connections). Fixing the first three changes public types. Everything below is reachable without a buggy application.

---

## F1 — The pump loop is O(connections) per wakeup, not O(events). **Critical.** [Q1]

**Scenario.** 10k idle h1 keep-alive connections, one shard, a few hundred active. `wait()` returns once per event batch; §5.2 then runs, unconditionally, `for (id, conn) in connections.iter_mut() { conn.poll(); …; conn.arm(&mut drv); }` over all 10k connections (design L159–163). At 20 ns per `poll`+`arm` (optimistic — `arm` is a heap insert/update) that is ~200 µs of scan per wait. To reach 200k events/s with 200-event batches it is 1000 waits/s → 0.2 core-seconds per second of pure scan; with 50-event batches it is 0.8. The cost is worst exactly where the 10k gate lives: many idle connections, moderate event rate, small batches.

**Breaks.** The 10k gate is a memory gate (L489, L521–533), but the architecture makes CPU scale with *idle* connections. The design's own framing — "one non-blocking recv per readiness event" (L94) — is not what the loop does; it is "one full scan of every connection per event batch". This is the first place readiness-under-load loses to an event-driven design, before any syscall argument: a per-connection state machine driven only by that connection's completions is O(1) per event.

**Root cause.** `Conn` is pull-only. The app cannot know which connection has output or an expired deadline without asking every connection, because `feed` records nothing in the driver and `poll`/`output` are the only probes.

**Smallest fix.** Give `feed`/`poll` a dirty bit the driver can track (either `feed` marks it, or the app calls `drv.mark_active(c)`), and make the second phase iterate a driver-owned `active: Vec<ConnId>`; `arm` is called when the driver delivers an event for that connection, not for every connection every loop. Public shape unchanged, cost O(events). If you keep the scan, the gate must be restated as "10k idle connections, and the reactor may not serve traffic while they are idle".

---

## F2 — Pool exhaustion is a busy loop on every platform; level-triggered readiness has no pause state. **Critical.** [Q1, Q4]

**Scenario.** The pool hits its 64 MiB cap (§7 L279). The drain loop is `while let Ok(mut chunk) = pool.get(READ_SIZE) { … }` (L140). `get` fails, the loop breaks, the app returns to `wait()`. Read interest is still on and the socket still has bytes, so level-triggered epoll/kqueue/AFD report it immediately; `wait` returns in microseconds; `get` fails again. With 10k readable sockets in this state the reactor spins at 100% per shard. If the cap is held by retained body chunks — the normal case under a slow-consumer attack — nothing frees, so it is a livelock, not a transient spin: the app can only free memory by processing other events, and it is spending its time re-discovering that it cannot read.

**Breaks.** §6.2 L196: "Backpressure is the absence of calls … There is no separate pause state to get out of sync." Absence of calls is not observable to the kernel; level-triggered readiness re-reports exactly the state you are trying to suppress. §7 L279's "self-healing backpressure signal, not an error path" is false for the same reason. §6.1 L184's "an application that ignores an event is slow, not broken" is wrong under load: ignoring a readable event is a spin. This is also the concrete answer to "where does readiness cost more than completions": a completion design gets backpressure for free by not re-posting a recv; here it costs a pause state, a per-driver paused list and a pool watermark, none of which the design has.

The vocabulary accidentally allows the fix (`interest` is a set), but the design denies the state machine and never says when reads resume.

**Smallest fix.** Make read-off first-class: clear `Interest::READ` on `Exhausted`, keep a `paused_for_memory` list in the driver, re-enable at a pool low-watermark checked once per loop, and delete the "no pause state" sentence. On AFD this doubles as the re-arm point (F12). The pool must expose `free_bytes()` or a watermark callback, or the app cannot know when to resume.

---

## F3 — One credit token per allocation is wrong for h2/h3: a read spanning two streams mis-attributes credit and leaks a window. **Critical.** [Q4]

**Scenario.** One 16 KiB TLS record contains h2 DATA: 12 KiB on stream 1, 4 KiB on stream 5 (interleaving is legal and normal). The engine splits the read into two `Data` events by slicing one chunk — §7 L305 sells exactly this as "0 copies, 1 refcount bump per stream". Both views share the single token in the allocation header, which names one stream and one byte count (L285). Stream 1's view drops first → the token fires "stream 1 consumed 16 KiB" → stream 1's window is over-credited by 4 KiB, and stream 5's 4 KiB is never credited. Stream 5 ratchets to zero with its bytes already consumed and the peer stalls that stream permanently; stream 1 holds window the connection reservation does not account for.

**Breaks.** Per-stream flow control (§9.3 L378) and bounded memory: over-crediting the connection window lets the peer send more than the reservation cap allows. The token is also arithmetically too small for h3: "16 bytes: owning connection, stream, byte count" (L285) leaves 4 bytes for a stream, but a QUIC stream id is a 62-bit value (quinn-proto `StreamId(u64)`). And "no atomic beyond the refcount" (L289) fails for any cross-thread drop — a `Send` chunk dropped by a worker or a second shard needs an MPSC push, which the worker body mailbox (L436) makes the common case.

**Cite.** §7 L269–271, L283–289, L293–306; §9.3 L373–379; §9.4 L381–386.

**Smallest fix.** Move the token from the allocation header into the *handle*: `Data` carries `Body { storage: Chunk, credit: Option<CreditToken> }`. `Chunk::slice` within one stream shares the parent's token via a second refcount; the engine's cross-stream demux constructs a second token (cost: one 24-byte refcounted header, only for reads that span streams) or copies the small piece. Widen `stream` to `u64`. Add the "split across streams: one token, not one copy" row to the copy table.

---

## F4 — Retained chunks outlive the connection by construction; §6.4's central sentence is false. **Critical.** [Q2, Q4]

**Scenario.** Proxy: client h2 conn A, upstream h2 conn B. A yields `Data{stream 5, chunk}` whose token names A/stream 5. The proxy passes the chunk to `drv.send(B, chunk)`; B's socket buffer takes 1.5 KiB of a 16 KiB chunk. The remainder is held by the proxy with `Interest::WRITE` (or by the driver, if `send` keeps partials — see F16). B's peer RSTs → `Io::Closed(B)`. The proxy drops the remainder → last ref drop → the pool returns storage **and pushes the token into A's credit queue**. That is a callback into a connection, from `Drop`, on whichever thread dropped it. If A also closed (simultaneous client abort), the token is pushed into a removed or freed connection, or into a `ConnId` slot already reused (if the sink does not check the generation). If the app leaked the remainder in B's close path, the token never fires and stream 5's window is never returned — the "never" case. If `drv.close`'s "retained chunks are dropped" is implemented as force-reclaim, the app's later drop is a refcount underflow and a double return.

**Breaks.** §6.4 L215: "There is nothing for the driver to drain on close that the application still holds … the application's own call is the only thing in flight." False for every zero-copy relay chunk, every partial send, every chunk handed to a worker mailbox (L436), and every chunk rustls or an engine holds between `feed` and `poll`. L212 "retained chunks are dropped" is impossible when the app holds the last reference. §13 L459–461 is the mechanism that makes this the normal path, not an edge case.

**Smallest fix.** Make the credit destination lifetime-independent: `Arc<CreditSink>` with an `AtomicBool closed`, or a per-driver token-recycle queue drained on the driver thread with a generation check. `close` sets closed and drops tokens; storage returns only via refcount; `ChunkPool` is `Arc`-owned by every chunk so a pool cannot be freed under a live chunk. State that close frees nothing the application holds.

---

## F5 — EOF has no input path: `Conn` cannot be told, the socket hot-spins, and half-close has no event. **Critical.** [Q2]

**Scenario.** Peer sends `close_notify` then FIN. `drv.recv` returns `Read::Eof`; §5.2 does `Read::WouldBlock | Read::Eof => break` (L147) and closes nothing. On Linux and macOS a socket with a pending FIN is readable forever; level-triggered `wait` returns it on the next call, `recv` returns Eof again, break — a 100% CPU loop per closed connection until something else closes it. Over TLS, `conn.feed()` takes a `Chunk`; there is no way to tell rustls "no more ciphertext", so `IoState::peer_has_closed()` is never consulted and `CloseReason::Peer` vs `Tls(truncated)` (L251–258, L420) is undecidable: a truncated close is reported as clean. For h1, a chunked body cut off at FIN is likewise indistinguishable. For CONNECT/`101`, `Read::Eof` is the only half-close signal and it only arrives from an application-initiated call; `Io` has no half-close event, so "half-close is forwarded in both directions" (L462) has no trigger.

**Cite.** §5.2 L147, §6.8 L246, §10 L420, §13 L462.

**Smallest fix.** `Conn::feed_eof()` (or `feed(Feed::Eof)`). `Read::Eof` means "call it, then keep polling for `End`/error". The driver latches `read_shut`, reports readable once more so the app can consume the EOF, then never again; `CloseReason` is derived inside `feed_eof` where the TLS state is. Tunnels need `Event::HalfClose` per direction.

---

## F6 — "Readiness can be backed by completions without changing semantics" is false for writes, and only half-true for reads. **High.** [Q1]

**Reads.** A *poll* completion (AFD, io_uring `POLL_ADD`) can back level-triggered readiness if the driver re-arms it. A *recv* completion cannot: the CQE consumes the event and the bytes are already in a library-private buffer. §6.1 L184 promises "an application that ignores an event is slow, not broken"; with a recv completion the library must keep that buffer and re-report "still readable" with no kernel readiness behind it. That is an operation lifecycle and a private pending queue — exactly what L98 says readiness avoids. Backpressure also inverts: to have a recv completion you must commit a buffer *before* readiness, so `pool.get` becomes eager (10k idle connections × READ_SIZE pinned) and `Exhausted` becomes "withdraw an outstanding recv" rather than a failure at call time. M4 keeps reads single-shot (L474), so it avoids this by not doing it — but then "completions underneath" only ever applies to writes.

**Writes: false, by the design's own argument.** §6.7 L231 correctly shows a zero-byte `WSASend` completes immediately even with `SO_SNDBUF` full; the same is true of `send(fd, NULL, 0)` on Linux. No completion tells you "writable" — a completion can only tell you "the data you already gave me went out". That is a new event, not a readiness edge. If `drv.send` returns after *submission* rather than after the bytes are in the kernel:

1. `send` returning no longer means "in the kernel", so `finish` → send → `shutdown_write` can put FIN before `close_notify` (F8). Nothing in the public API lets the caller distinguish.
2. The chunk must be retained by the driver until the second `SEND_ZC` CQE, so ownership of the bytes moves from the socket to a ring. `pool.get` can now fail because of a send that "completed", and the §15 memory budget has to count kernel-pinned buffers that §7's table does not.
3. Close must not wait for those retained chunks (L474) while they are the only thing holding the body and, in the relay, the inbound credit token (F4).
4. `drv.send` needs an outcome richer than `Result<()>`. The anti-goal "two APIs for the same thing" (L47) is triggered unless that richer outcome is the API from day one.

**Smallest fix.** Define the outcome now: `Sent::{All, Pending}`, or `send` plus `Event::Flushed`, and make `finish`/`shutdown_write` wait on "flushed". State that the readiness contract is backed by *poll-shaped* completions only, and that I/O completions (M4) are an explicitly added capability which extends the contract — not something that hides under it. Add the per-connection readiness latch: a driver bit set by kernel readiness or by "I hold undelivered decrypted bytes", cleared only by a syscall returning `WouldBlock`; that makes LT real on AFD (F12) and fixes F7.

---

## F7 — `Exhausted` after the kernel buffer is drained strands plaintext; TLS makes the "self-healing backpressure" claim false. **High.** [Q4, Q1]

**Scenario.** A full 16 KiB TLS record is read into rustls' internal buffer via `read_tls`, `process_new_packets` produces 16 KiB of plaintext, and the plaintext chunk's `pool.get` fails at the cap. No bytes remain in the socket. The app breaks (§5.2 L140–150); level-triggered readiness has nothing to re-report; the peer is waiting for a response. Deadlock until the peer sends more — which for a complete 1 MiB POST it will not.

**Breaks.** §7 L279: "the socket keeps its bytes … level-triggered readiness re-reports". True only before the record is read. The copy budget (L299) is the cause: ciphertext is copied out of the chunk into rustls before the plaintext chunk is acquired.

**Smallest fix.** Acquire the plaintext chunk (or a per-connection staging reservation) *before* `read_tls`/`process_new_packets`; on failure leave the ciphertext in the socket. If the layering forbids that, the connection holds the decrypted bytes and the latch from F6 reports readable without a syscall.

---

## F8 — `shutdown_write` cannot be ordered after `close_notify`/GOAWAY, and graceful drain has no defined deadline. **High.** [Q2]

**Scenario (ordering).** The app calls `finish()` (queues `close_notify`), loops `while let Some(out) = conn.output() { drv.send(…) }`, then `drv.shutdown_write(c)`. `output()` returning `None` means "no framed output right now", not "all bytes are in the kernel" (L317); on the readiness path `send` may have taken a partial, on the M4 path the bytes are only submitted (F6). The driver does not own `Conn` and cannot flush it, so §6.5 L219's "after the TLS layer has emitted `close_notify`" is unenforceable: FIN can precede `close_notify`, which the peer must treat as truncation. For h2, `output()` "returns nothing while flow control forbids sending" (L317); a GOAWAY enqueued by `finish` behind queued DATA sits behind a zero stream window, so `finish` produces nothing at all.

**Scenario (hang).** Peer holds its stream window at 0 but keeps sending PINGs. Whatever idle-*read* deadline the connection armed keeps being refreshed, so "the socket stays readable until the peer closes or its deadline fires" (L213) never fires. The connection and its streams are held forever, and in a proxy the paired leg with it. §9.3 L379 leans on "the deadline is what closes it" without ever naming that deadline.

**Cite.** §6.4 L212–213, §6.5 L219, §8 L317, §14.5 L474, §9.3 L379.

**Smallest fix.** `finish()` returns a `Drain` token; `drv.shutdown_write(c, drain)` is legal only after `conn.poll()` yields `Event::Drained` (no framed output, TLS internal buffer empty, no pending send notifications). Control frames are never flow-controlled and are spliced at the head of `output`. `finish` installs its own deadline in the driver, keyed on last-write-progress, not on reads.

---

## F9 — Timers: generation covers slot reuse, not re-arm; close does not purge; `Closed` is not last; the app cannot route `Io::Timer`. **High.** [Q3]

**Scenario (stale by re-arm).** `wait` returns with a deadline due at T; the app processes a `Readable` first and calls `conn.arm`, which *updates* the deadline to T+50 ms; `next_io` then yields the `Io::Timer` already generated at T. A generational `TimerId` that changes only on slot reuse does not make that event stale, so the connection sees a deadline it already extended and closes or resets prematurely. L202's "disarmed or re-armed between the wait and the drain is recognised as stale" needs a version bumped on every arm, not on reuse.

**Scenario (Closed not last).** The connection is armed with a 30 s idle deadline; the peer RSTs at t=1 s; `Closed(PeerReset)` is delivered; at t=30 s the timer fires. Nothing says `close` purges the heap. `Io::Timer(TimerId)` carries no `ConnId` (L248), so the app must maintain a `TimerId`→`ConnId` mirror of the driver's heap — and the mirror is precisely what goes stale. If the app indexes its map that is a panic; the design promises "an error, never a panic" (L261).

**Scenario (queued events).** One `wait` produces `Readable(A)` and `Writable(A)`. The app closes A on the readable (fatal parse error, or `feed` Err); the already-queued `Writable(A)` is delivered after `Closed(A)`, breaking L246's "always the last event for that id". §5.2 also uses `?` on the main loop (L141, L144), making `recv`/`feed` errors a third close channel alongside `Io::Closed` and `drv.close` — the design never says which one wins.

**Smallest fix.** Fold a per-connection arm counter into the timer event; `close` purges the heap and marks the id dead; `next_io` filters every event against the live table before yielding and emits `Closed` at the tail. Change `Io::Timer(TimerId)` to `Io::Timer(ConnId, TimerKind)` so the app never mirrors the driver's table. Pick one close channel and make the other two set state rather than deliver a second `Closed`.

---

## F10 — No public way to set a deadline; the proxy cannot time out a tunnel or a slow body. **High.** [Q3, Q2]

**Scenario.** CONNECT tunnel; the client half-closes (`SHUT_WR`); the upstream never sends and never closes. §13 L462 promises "until it too closes or times out". There is no API to arm that timer: `conn.arm(&mut drv)` is the connection's own call with its own deadlines, and `Deadline`/`TimerId` are not publicly constructible. The same gap applies to a handler reading a body slowly and to the write-progress deadline §9.3 L379 depends on. 10k half-open tunnels × 2 sockets is an fd-exhaustion DoS the memory gate will not catch.

**Breaks.** Requirement §2 ("bounded queues, deadlines, and cancellation") and §8's "slow request bodies and slow consumers need explicit deadlines". The design keeps timers private and offers no application-facing deadline; `wait` owns the timeout, so the app cannot even bound its own latency for periodic work.

**Smallest fix.** `drv.set_deadline(conn, DeadlineKind, Instant)` with kinds `Idle | ReadProgress | WriteProgress | Drain | HalfClose`; `arm` merges engine deadlines with these; `drv.timer(Instant) -> TimerId` for app-level ticks. Add a work quantum: cap events per `wait` and re-derive the timeout, or a 10k-event batch makes a 1 ms deadline fire 100 ms late — §6.3's "the earliest timer always fires" is true per wait call, not per event.

---

## F11 — The worker `Deadline` is a second timer owner; cancellation after reset is undefined. **High.** [Q3, Q5]

**Scenario.** A worker parks on the body mailbox with `Deadline::after(100ms)` (L432). At the same instant the pump fires the connection's idle deadline, resets the stream and returns its reservation (L434). The worker wakes with its own `TimedOut` and writes a 1 MiB response into a writer whose buffers were "returned". §6.3 L202 claims this race does not exist because "the pump fires the deadline and *it* is what cancels the parked work", but nothing says the worker's `park_timeout` is not a second clock. If instead the deadline is registered in the driver's heap, the worker touches the driver's timer structure — which L436 forbids ("a worker never sees … a driver") — and the heap becomes a cross-thread structure, so "exactly one owner" becomes "one lock".

A worker blocked in its own database (allowed by the requirements) cannot be cancelled at all; it returns later and writes. The `Exchange`, reader and writer have no stated post-reset behaviour.

**Smallest fix.** `Deadline` is an opaque driver-issued value; the worker's park has no local timer; only `wait` evaluates time and wakes with a typed reason. `Exchange`/reader/writer check a shared cancel state and return `Err(Reset)` after reset — a contract, not a hope. State that user-code blocking is uncancellable and the stream is reset regardless. Define the cap-exceeded response (h2 RST_STREAM vs h1 503); "reset rather than queued" is a client-visible policy.

---

## F12 — Windows: AFD is a correctness dependency with an insufficient probe, and the WSAPoll fallback cannot pass the design's own gate. **High.** [Q6]

**Scenario (semantic change).** An AFD change that still lets `\Device\Afd` open — an event-mask bit, a level/edge change, a struct-layout addition — is not detected by "if the AFD handle cannot be opened" (L235). The driver then silently fails to re-report readiness or misses abort: connections stall with no error and no fallback. That is lost **correctness**, not throughput, and it contradicts §17 L581.

**Scenario (fallback gate).** `WSAPoll` marshals the whole array on every call and scans it O(n) in the kernel (the design's own Appendix A). 10k sockets is an 80 KiB copy and a 10k-entry scan per wait; with any timeout it is O(n) per wake; with a 1 ms poll it is 10M checks/s. Gate 1 requires "throughput ≥ 95% of hyper" and "10k idle connections per reactor thread" **on every platform** (L489, L510–515). Hyper on Windows uses mio → AFD, so the fallback cannot meet the gate by construction, and §17's "the design still holds; the numbers do not" is false: the design's own pass condition *is* a number.

**Also.** AFD readiness is one completion per outstanding poll and must be re-issued; §6.1 L188's "there is no re-arm step" is false on Windows. That missing re-arm is what turns F2's pool-pause from a spin (Linux/macOS) into a permanent stall (Windows).

**Smallest fix.** A startup self-test of AFD semantics (socketpair: read, write, abort, and *re-report after not draining*) with automatic fallback on any mismatch. Record which Windows path each gate number came from; scope Gate 1's Windows entry to AFD-present, or write the explicit waiver for the fallback and add a fallback-specific pass condition that measures O(n) poll cost rather than pretending it meets the same bar.

---

## F13 — It is an executor; the lie is about scheduling, not about spawning. **Medium.** [Q5]

**Where the lie lives.** §4.1 L80–81: "No hidden control flow at all; the library cannot be blamed for scheduling decisions it did not make." The library makes four scheduling decisions the app can neither see nor change: queue admission order (L430), per-connection parked-worker cap and the reset-on-excess policy (L433), timer expiry order (L200), and cross-thread wakeup batching (L271). `drv.wake()` (L431) is a `Waker` with a different name. The app owns thread *creation*; the library owns *dispatch*. This is a defensible design — but it means the "no hidden executor" property is delivered only to users who don't enable `threads`, i.e. almost nobody.

**Also unspecified (requirements §10 Q4 asked explicitly):** shared vs per-shard worker queue. With 2 shards and 8 workers, a worker holding an exchange from shard 2 calls `drv.wake()` — whose? There is no `WakeHandle` type, and `Server`'s topology is unstated, so a response is either delivered to the wrong shard or serialized through one pump.

**Smallest fix.** Name it: a `Dispatcher` behind `threads` that owns queues, caps and the timer heap; expose `WakeHandle` (`Send + Sync`, `wake()` only, one per shard); state shared-vs-per-shard and the cap value. State the invariant "the pump runs no application code" and enforce it in `Server` (debug assertion that the pump thread never enters a handler). Without that invariant every timing claim in §6 is conditional on a rule nothing checks.

---

## F14 — `Event::Head`/`Fields` ownership is undefined; "identity plus span" needs an owner. **Medium.** [Q4]

**Scenario.** A 4 KiB read contains request headers plus the first body bytes. The engine yields `Head` (field values as spans) and `Data` (a chunk view of the same storage). The proxy stores the `Head` to re-emit after the upstream connect, and the `Data` view drops. If `Head` holds raw ranges and does not bump the allocation refcount — which is what "identity plus a span" (L403–408) and `HeaderValue` as "a validated byte range" suggest — the storage returns to the pool and the headers are read from recycled memory when re-emitted. If instead each `Head` copies, that is an allocation per request, contradicting §15's "avoid per-request heap allocation" and absent from the copy table.

**Cite.** §9.5 L401–410, §8 L338; requirement §8 L216–219 asked for exactly this ("a self-referential head API does not typecheck … owned head (arena + spans) and a borrowed view").

**Smallest fix.** `Head` owns an `Arc`/`Chunk` (or an arena); `HeadView<'_>` is reconstructed per access. Add the row to the copy table ("header/body split: 0 copies, 1 refcount"). Say what the worker's `Exchange` owns.

---

## F15 — h3: the copy budget is understated and connection lifecycle has a second channel. **Medium.** [Q4, Q2]

**Scenario (copies).** §9.4 L386 accounts one inbound copy per datagram (chunk → `BytesMut`) and one per outbound transmit batch. The h3 *stream* receive path is unaccounted: quinn-proto yields stream bytes as its own `Bytes`/`Chunk`, which cannot be adopted by a pooled libhttp chunk, so every inbound stream chunk costs another copy into the pool that the §7 table does not list. Requirement §5 demands the real budget at the quinn boundary.

**Scenario (lifecycle).** h3 connections are not `ConnId`s (L397), so `Io::Closed`/`CloseReason` cannot apply to them: the endpoint is a second lifecycle source with no `Closed`, no `CloseReason` and no defined `h3::ConnId` validity, while §9.1 L362 says lifecycle is reported by the driver and §14.1 L471 claims "no operation ids … so there is no id lifetime to define". The proxy's h1↔h3 and h3↔h3 pairs (L458) need a close reason to choose 502 vs retry.

**Also.** quinn-proto returns stream flow-control credit when the application `read`s, not when it consumes the chunk, so §9.3 L378's "stream window returned only when the application consumes" requires the engine to read at most one chunk ahead per stream. That rule must be written or the h3 credit policy silently becomes read-time.

**Smallest fix.** Add the stream-receive copy row and measure it; give `h3::Endpoint` a `poll` yielding `Closed(h3::ConnId, CloseReason)` with a generational id; state the one-chunk read-ahead rule.

---

## F16 — Smaller defects (each real, each cheap). **Low–Medium.**

- **accept→insert window.** `drv.accept` arms read interest immediately (L137–138); if `Conn::new` or the app's insert fails, the socket is live in the driver and absent from the app's `connections`, so `connections[c]` panics — "never a panic" (L261) violated by the design's own index. Fix: `accept` returns an inactive connection; read interest starts on `drv.activate(c)`.
- **Pool classes vs TLS records.** Classes 2/16/64 KiB plus a 16-byte headroom (L273–275): a 16 KiB plaintext record does not fit the 16 KiB class, and a 16 406-byte ciphertext record fits neither; every full TLS record lands in the 64 KiB class, so the 64 MiB cap holds ~1 000 records, not 4 000. Fix: classes are *usable* sizes excluding headroom, add a ~17 KiB class, or restate the memory math.
- **Token recycling race.** `Chunk::drop` returns storage to the free list and then pushes the token read from the header (L286). Another thread can `pool.get` the same storage and overwrite the header first. Copy the token out before publishing the block.
- **Partial sends have no contract.** `drv.send(*id, out)?` (§5.2 L161) consumes the chunk; a partial write must either be kept in a driver-owned per-connection pending queue — an output queue the design says does not exist — or returned, which means `send` needs `Sent::Partial(Chunk)`. That is also the seam M4's batched and `SEND_ZC` writes need. As written the app cannot implement a correct pump loop from the public API.
- **Gate blind spots.** Gate 1's two endpoints (trivial GET, 1 MiB body through the proxy, L508) never exercise the worker body mailbox, the cross-thread chunk return, the pool-paused state, or any completion baseline. A-vs-hyper measures the design against another readiness design, so "completions only where a benchmark proves it" has no baseline that could ever prove it. Add a streaming-upload-through-a-worker endpoint and a paused-pool endpoint, and write down the per-read syscall floor (2 KiB reads = one syscall per 2 KiB) as the number to beat.
- **`Io::Settings` synthesised for h1** on every connection (L357) means every h1 relay leg gets a fake peer-settings event it must ignore; harmless, but it makes "the same event vocabulary" noisier than claimed.

---

## Coverage

| Question | Findings |
|---|---|
| Q1 readiness under load | F1, F2, F6, F7, F12 (and F16's syscall floor). Sharpest is F1: the per-read syscall floor is inherent and acknowledged; the O(all-connections) loop is not. |
| Q2 close / half-close / drain | F4, F5, F8, F9 (and F16's partial sends) |
| Q3 timers / cancellation | F9, F10, F11 |
| Q4 buffers / credit | F3, F4, F7, F14, F15, F16 |
| Q5 "no hidden executor" | F13, F11 |
| Q6 Windows | F12 (F2/F6 for the level-trigger consequence) |

No category is empty.

## Checked and not faulted

- The AFD-vs-zero-byte-`WSASend` reasoning (§6.7) is correct: a zero-byte send is not a writability probe on any platform, so a *send-completion* backend genuinely cannot back a readiness contract for writes. The problem is that §4.3 and §14.5 still sell that backend as an invisible optimisation.
- kqueue is level-triggered by default; `EV_CLEAR` is the edge mode. The Linux/macOS half of §6.1 is right.
- The rustls copy accounting (2 in, 1 out) is honest for the 0.23 `read_tls`/`write_tls` surface.
- quinn-proto as the only pure-Rust sans-I/O QUIC on the stated criteria is defensible.

## If only four things change

1. **F3 + F4** — token per view, credit sink that outlives the connection. Correctness; no protocol layer can be written correctly on top of the current contract.
2. **F1 + F2 + F6 + F7** — driver-owned active set, clearable read interest with a pool watermark, per-connection readiness latch, pre-acquire before `read_tls`. One mechanism family fixes the scan, the spin and the TLS stranding.
3. **F5 + F8** — `feed_eof`, and a `Drain`/`Drained` contract for `finish`/`shutdown_write`. Half-close and `close_notify` are requirements, not details.
4. **F9 + F10 + F11** — one clock, application-settable deadlines, `Io::Timer(ConnId, kind)`. These three are one design decision.
