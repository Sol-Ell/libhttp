# libhttp — Design

**Status:** rewritten after three verification passes. Contracts are stated once each; everything else points at them.
**Date:** 2026-10-26.
**Companion:** `2026-10-26-libhttp-requirements.md` (the brief). The archived revisions were not read.

**How to read this.** §6–§13 are normative: they define the public contracts. Every contract appears in exactly one place. Where another section needs it, it references the section rather than restating it — the previous revisions contradicted themselves precisely because the same contract was written four times. §5's worked example is illustrative and explicitly not normative. §16 is the only place where a decision is still open.

---

## 1. Intent

libhttp is a Rust HTTP library — not a framework, not a runtime. It exists because the available stacks force an abstraction (a body trait, a service tower, an async runtime) and hide the copies and wakeups that decide performance.

The bet is that **explicitness and speed are the same thing here**: if every copy, allocation, wakeup and syscall has a name and an owner, the fast path stops being a special case. Three sentences describe the library:

- The application owns its threads and its event loop; libhttp owns sockets, buffers, timers and protocol state.
- The public I/O contract is readiness; completion-based I/O is a submission path, never a readiness substitute.
- Protocol engines are sans-I/O byte machines behind one event vocabulary, so an h1↔h3 relay is the same code as h2↔h2.

---

## 2. Goals, non-goals, anti-goals

**Goals.** HTTP/1.1, HTTP/2, HTTP/3, client and server, over TLS, plus a reverse proxy built from public pieces only. Synchronous application code. No `Future`, `Waker`, executor or runtime anywhere, including TLS and QUIC. No thread per connection, no blocking socket I/O on an I/O thread, no threads spawned by the core. Identical public API on Linux, macOS/BSD and Windows. A copy/atomic/syscall budget a reader can check.

**Non-goals.** A web framework (no routing, extractors, middleware). Tokio or any async-runtime integration. HTTP/2 push and priority. HTTP/3 connection migration, 0-RTT, Alt-Svc, Happy Eyeballs. A general-purpose I/O library — the reactor speaks HTTP's transport, not every protocol's.

**Anti-goals.** A fast path that hides work (batching that silently buffers, a pool that silently allocates, a wakeup that silently crosses threads). Two APIs for the same thing. Platform types in a signature. A number bought with correctness. A proxy that needs a private module.

---

## 3. Constraints

- No `async`/`.await`, no `Future`, `Waker`, or hidden executor — not for TLS, not for QUIC, not internally.
- No blocking socket I/O on an I/O thread; no thread per connection; no unbounded queue.
- Linux, macOS/BSD and Windows first-class; CI builds and runs the functional suite on all three; numbers are published per platform.
- Typed protocol model: `Method`, `StatusCode`, `Version`, typed header names and values, parsed targets. No `String`/`HashMap<String, String>` on the hot path.
- No `bytes`-style buffer implementation, no `httparse`, no `hyper`, `h2`, `h3`, `tokio` in the product.
- OS crates (`libc`, `windows-sys`, `io-uring`) are allowed. rustls for TLS. QUIC transport delegated; HTTP/3 framing and QPACK ours.
- The core spawns no threads; the only spawning is the convenience layer, behind a feature.

---

## 4. Decisions and what they beat

| Decision | Chosen | Rejected, and why |
|---|---|---|
| Who owns threads | The application owns threads and the loop; the library owns dispatch | A library-owned reactor and pool is a hidden executor with extra steps |
| Public boundary | Packets and semantic events | Exchanges hide the flow-control, reset and half-close mechanics the proxy must drive |
| I/O contract | Readiness, level-triggered | Completion-shaped: unrepresentable on kqueue without a helper thread and an operation lifecycle the caller manages |
| I/O acceleration | Completions for submission only (batch datagrams, file bodies) | Completion-backed `send` cannot report writability — no platform offers a probe |
| Credit | Consumption-based, one public handle | Read-time credit lets a slow consumer pull the peer's window open |

---

## 5. Architecture

### 5.1 Layers

```
  application        threads, pump loop, handlers, the proxy
  ─────────────────────────────────────────────────────────────
  convenience        Server / Client / Workers            thin, feature-gated
  ─────────────────────────────────────────────────────────────
  connection         Conn: TLS + one protocol engine       feed/poll/output
  ─────────────────────────────────────────────────────────────
  protocol           h1 / h2 / h3 engines                  sans-I/O, shared events
  ─────────────────────────────────────────────────────────────
  transport          Driver: sockets, readiness, timers, datagrams
  ─────────────────────────────────────────────────────────────
  platform           epoll · kqueue · AFD/IOCP   (+ io_uring submission, phase 2)
```

Dependencies point down only. The transport does not know what HTTP is; protocol engines do not know what a socket is.

### 5.2 The model, as a non-normative example

This is a sketch of one correct loop. The contracts it uses are §6–§13; if they disagree, they win.

```rust
let mut drv = Driver::new(Config::default())?;
let listener = drv.listen("0.0.0.0:8080".parse()?)?;

loop {
    drv.wait()?;                                     // event, timer, or batch budget

    while let Some(io) = drv.next_io() {
        match io {
            Io::Acceptable(l) => {
                let (c, _peer) = drv.accept(l)?;      // inactive until activate
                conns.insert(c, Conn::new(c, Protocol::Detect, None));
                drv.activate(c)?;
            }
            Io::Connected(c)        => { conns[c].start()?; drv.mark_active(c); }
            Io::ConnectFailed(c, _) => { /* retry or fail, per policy */ }
            Io::Readable(c)         => read_until_stalled(&mut drv, &mut pool, &mut conns, c)?,
            Io::HalfClose(c)        => conns[c].feed_eof()?,
            Io::Writable(c)         => { conns[c].flush_tail(&mut drv)?; drv.mark_active(c); }
            Io::Timer(c, kind)      => { conns[c].on_deadline(kind)?; drv.mark_active(c); }
            Io::Closed(c, _reason)  => { conns.remove(c); }
            Io::Datagram(u)         => { /* h3: hand to h3::Endpoint */ }
        }
    }

    for c in drv.active() {
        let Some(conn) = conns.get_mut(c) else { continue };   // closed earlier in this batch
        conn.tick(&mut drv)?;                         // credit released here may need output
        while let Some(ev) = conn.poll() {
            if matches!(ev, Event::Drained) { drv.shutdown_write(c)?; }
            route(ev);
        }
        while let Some(out) = conn.output() { conn.outbox.push_back(out); }
        while let Some(front) = conn.outbox.front() {
            match drv.send(c, &[front.clone()])? {
                Sent::All => { conn.outbox.pop_front(); }
                Sent::Partial(n) => { conn.outbox.advance(n); break; }   // resend the remainder
                Sent::WouldBlock => break,
            }
        }
        conn.arm(&mut drv)?;                          // engine deadlines into the driver heap
    }
}

// Stops on WouldBlock, EOF, a spent byte budget, or pool exhaustion.
fn read_until_stalled(drv, pool, conns, c) -> Result<()> {
    let mut budget = READ_BUDGET;
    while budget > 0 {
        let mut chunk = match pool.get(READ_SIZE) {
            Ok(chunk) => chunk,
            Err(Exhausted) => return drv.pause_reads(c),
        };
        match drv.recv(c, &mut chunk)? {
            Read::Data(n) => {
                budget = budget.saturating_sub(n);
                chunk.truncate(n);
                conns[c].feed(chunk.freeze())?;
                drv.mark_active(c);
            }
            Read::WouldBlock => return Ok(()),
            Read::Eof => { conns[c].feed_eof()?; drv.mark_active(c); return Ok(()); }
        }
    }
    Ok(())
}
```

`READ_SIZE` is 16 KiB; `READ_BUDGET` is 256 KiB per readiness event.

---

## 6. Transport contract

### 6.1 Readiness

Readiness is **level-triggered** on every platform: `wait` reports a socket while it has readiness to report, and the application never re-arms anything. Readiness is a hint — `recv` after a `Readable` may return `WouldBlock`, and that is an ordinary result, never an error.

`recv` and `send` are non-blocking by contract. The driver never parks the calling thread; the application's loop does all waiting, once, in `wait`.

`wait` returns when an event is available, a timer is due, or the **batch budget** is spent. The budget bounds events per call, not time: one event can consume `READ_BUDGET` (16 KiB × 16 reads) plus TLS and parsing, so a deadline's lateness is bounded by `budget × per-event work`, and the measured lateness is reported by the benchmark, not asserted here.

### 6.2 Interest, pause and backpressure

`drv.interest(conn, Interest)` sets what is reported. A connection accepted or connected has **no** read interest until `drv.activate(conn)`, so no id can arrive for a connection the application has not inserted yet. Write interest is enabled only while a `send` has returned `Partial` or `WouldBlock`, and cleared when the tail drains.

Backpressure is the absence of calls — with one exception. **Pool exhaustion clears read interest** and parks the connection; the pool's return path re-arms interest at a low watermark (25% of the cap), in one batch. Without that, level-triggered readiness would re-report the same socket immediately and spin. `drv.paused()` makes the state observable.

Stopping reads is connection-granular by nature. It is therefore never used as a per-stream refusal (§9.2) and never as a worker-cap response (§11).

### 6.3 Sockets, ids and ownership

The driver owns an authoritative, mutable table of listeners, connected sockets and UDP sockets. `ConnId`, `ListenerId`, `UdpId` are generational handles; a stale handle is an error, never a use-after-free, and never a panic.

The application never sees a file descriptor and cannot register its own socket. `listen`, `connect`, `accept` and `bind_udp` create sockets; there is no adoption path in v1, because adoption is where a portable API starts leaking `fd`s.

### 6.4 Close, half-close and drain

Two operations, because abort and drain are different things.

- `drv.close(conn)` — abort. Remove the socket, purge its timers, mark its credit sink closed, deliver one `Closed`. It does **not** free storage the application holds; it cannot, and force-reclaiming it would underflow the refcount when the application's copy drops.
- `conn.finish()` — graceful. It queues the final output (`close_notify`, h2 GOAWAY). The application drains `output()` and then calls `drv.shutdown_write(conn)`.

`shutdown_write` half-closes: `shutdown(SHUT_WR)` on Unix, `shutdown(SD_SEND)` on Windows, after TLS has emitted `close_notify`. Half-close is required for CONNECT and `101` tunnels and is not an optimisation.

The drain has two active conditions, and each has exactly one owner:

- The **connection** knows when it has no more output to produce, and reports `Event::Drained` once `finish()` has been called and `output()` has returned `None`.
- The **application** knows whether it still holds the tail of a partial send. `shutdown_write` is legal only when it does not.

The driver cannot compute `Drained` for the second reason; it never tries. Calling `shutdown_write` with a tail outstanding truncates the stream, and the library cannot detect it — that is why the condition is stated as a precondition rather than enforced.

### 6.5 Timers and deadlines

The driver owns one timer heap, and it is the only clock.

- `conn.arm(&mut drv)` upserts that connection's engine-owned deadlines.
- `drv.deadline(conn, kind, after)` sets an application deadline of a stated kind.
- `drv.wait()` derives its timeout from the heap, so no call site can pass a wrong one.

Every arming bumps a counter the driver keeps. At delivery the driver compares it and drops the event if the timer was re-armed or purged since; the counter never reaches the application, which is why `Io::Timer` carries only the connection and the kind. `close` purges the heap, and no event is delivered for an id after its `Closed`.

A worker owns no clock: it parks on a schedule the pump drives, and the pump is what fires the deadline (§11).

### 6.6 Datagrams

UDP sockets are transport primitives from M1, not a later h3 appendage: `bind_udp`, `recv_datagrams(&mut [Datagram])`, `send_datagrams(&[OutDatagram])`, batched. `Datagram` carries the payload plus the peer and, where the platform genuinely provides it, ECN — otherwise `None`. The library never fabricates metadata; §B lists what each platform actually has.

### 6.7 The `Io` vocabulary

```rust
enum Io {
    Acceptable(ListenerId),
    Connected(ConnId),                      // non-blocking connect finished
    ConnectFailed(ConnId, ConnectError),    // refused, unreachable, timed out
    Readable(ConnId),
    HalfClose(ConnId),                      // peer finished writing; we may still write
    Writable(ConnId),
    Timer(ConnId, TimerKind),
    Closed(ConnId, CloseReason),            // exactly once, always the last event for the id
    Datagram(UdpId),
}

enum TimerKind { Idle, ReadProgress, WriteProgress, Drain, HalfClose }

enum CloseReason {
    Local,               // we closed or finished the drain
    Peer,                // clean EOF; with TLS, close_notify was seen
    PeerReset,           // RST, or a QUIC reset
    TimedOut(TimerKind),
    Tls(TlsError),       // alert, or a truncated close — an error, never a clean EOF
    Io(IoError),
}
```

`HalfClose` is not `Closed`: a peer that half-closes leaves the connection writable, and conflating the two is how relays tear down tunnels that are still live.

### 6.8 Backends

One internal `Backend` trait: register, reregister, deregister, wait, next, wake. Three implementations.

- **Linux:** epoll, level-triggered (never `EPOLLET`); `epoll_ctl` only when the interest mask actually changes.
- **macOS/BSD:** kqueue with `EVFILT_READ`/`EVFILT_WRITE`, level-triggered (no `EV_CLEAR`).
- **Windows:** `IOCTL_AFD_POLL` against `\Device\Afd`, the interface mio 1.x and wepoll use, completions drained through the same IOCP with `GetQueuedCompletionStatusEx`. AFD polls are one-shot, so the **backend** re-issues one after each completion. This is an internal NT interface: §17 records the risk, and a startup self-test verifies semantics (read, write, abort, and re-report after not draining) before the backend is used. The documented fallback is `WSAPoll` for read/write readiness plus `ConnectEx` for connect, because `WSAPoll` never reports a failed connect.

Zero-byte completion probes are not used for readiness: a zero-byte `WSASend` completes immediately even with `SO_SNDBUF` full, so write readiness cannot be probed that way.

---

## 7. Buffer contract

### 7.1 Chunk and pool

`Chunk` is a handle to pooled storage: storage, range, one atomic refcount, and the pool it came from.

- `ChunkMut` (from `pool.get`) is uniquely owned; `freeze()` converts it to a shared `Chunk`. Writing requires ownership, and the type system enforces it.
- `Chunk: Clone + Send`, not `Sync`. `clone` and `slice` share storage and bump the refcount — **the count is atomic on every path**, because a clone can cross a thread while another stays. There is no sound same-thread plain-count case; an earlier claim that there was one is withdrawn.
- Dropping the last reference returns storage to the owning pool; cross-thread returns batch into that pool's inbound queue, one wakeup per batch.
- Class sizes are *usable* sizes — 4 KiB, 17 KiB, 65 KiB — with 16 bytes of headroom and 2 of tailroom added on top, so a full 16 KiB TLS record fits the second class. The pool is bounded by total bytes, and `pool.get` fails at the cap rather than allocating past it.

Framing never writes into a shared allocation. A read slice may begin at offset 0 of its storage with another stream's payload immediately before it, so headroom on the *allocation* is not headroom on a *slice*. `send` therefore takes a **chain** of chunks: the engine emits a frame header as one segment and the payload as the next, the kernel gathers them with one `writev`, and nothing is copied to make room. Headroom is only for chunks the engine allocates itself, which is where a length is back-filled.

### 7.2 The body handle and credit

Consumption is what returns flow-control credit — for h2 and h3 alike. The public type is one handle:

```rust
struct Body {
    chunk: Chunk,            // body bytes; may be a slice of a shared read
    stream: u64,             // h2 and h3 stream ids are both 62-bit
    consumed: u32,           // bytes this handle is responsible for
    sink: Arc<CreditSink>,   // the connection's credit queue + generation + closed flag
}
```

- `Event::Data { stream, body }` carries it. A body can be split with `body.split_at(n)`, which yields two handles over one storage — still no copy, and each carries its own consumption count.
- Dropping the last handle for a given `(stream, bytes)` credits the sink. `send_data` consumes the handle and credits on delivery to the destination stream, which is what makes a relay's credit travel with the data.
- `CreditSink` outlives the connection: it carries a generation and a closed flag, so a handle dropped after `close` credits a dead sink instead of a recycled `ConnId`.
- `conn.tick()` drains the sink and returns credit to the engine. **`tick()` runs before the output drain** (§5.2), because returning credit can itself require output — an h2 `WINDOW_UPDATE`, or an h3 `ShouldTransmit`.

h3's mechanism differs under the hood (§9.5) but the public rule is the same one: the handle is consumed, and only then does the peer learn it can send more.

### 7.3 Copy and atomic budget

| Path | Copies | Atomics |
|---|---|---|
| Plaintext body, socket → engine | 1 (kernel → pooled chunk) | 2 (allocate, drop) |
| Plaintext body, engine → socket | 1 (chunk → kernel) | 2 |
| TLS inbound | + up to 2 (chunk → rustls buffers → plaintext chunk) | 2 |
| TLS outbound | + 1 (plaintext → rustls buffer → ciphertext chunk) | 2 |
| h3 inbound datagram | + 1 (chunk → `BytesMut`) | 2 |
| h3 inbound body byte | + 1 (quinn `Chunks` → pooled chunk) | 2 |
| h3 outbound transmit batch | + 1 (`Vec` → pooled chunk), once per batch | 2 |
| Header/body split from one read | 0 | +1 (the `Head` keeps the arena alive) |
| Proxy body relay, same shard | 0 | +1 per split |
| Proxy body relay, cross shard | 0, plus one batched return | +2, and one atomic on the sink |
| Body split across h2 streams | 0 | +1 per split, one credit charge each |

One expectation is stated rather than hidden: the TLS numbers are the cost of rustls' sans-I/O API, measured at M2, and an in-place deframer is a named follow-up if the measurement is bad. The proxy body path copies zero times or this design is wrong.

---

## 8. Connection contract

A `Conn` is TLS plus one protocol engine. Its whole surface:

```rust
conn.start()                        // begin the handshake; no-op for cleartext h1
conn.feed(chunk) -> Result<()>      // ciphertext in; decrypts, parses, retains refcounted spans
conn.feed_eof() -> Result<()>       // no more will arrive; the only way a truncated close is caught
conn.poll() -> Option<Event>        // protocol events, plus Ready and Drained
conn.output() -> Option<Chunk>      // framed and encrypted, ready for the socket
conn.finish()                       // queue close_notify / GOAWAY
conn.tick(&mut Driver)              // release consumed credit; may enqueue output
conn.arm(&mut Driver)               // engine deadlines into the driver heap
```

### 8.1 Output admission

`output` is not one FIFO, and without the rule below this design deadlocks:

- a stream's pending body is admitted only when that stream's window covers it;
- control frames and QPACK encoder/decoder bytes are **never** gated by a request stream;
- a header block is admitted only after the encoder instructions it references.

A single ordered queue that a blocked stream can fill would put an h2 `PING` ACK or a GOAWAY behind a zero window the peer itself set. With QPACK at capacity zero there are no encoder instructions to order, but the rule is stated now because the dynamic table lands at M5 and the requirement is not optional then.

### 8.2 Choosing the protocol

A connection is created with a policy, not an engine:

```rust
enum Protocol { Http1, Http2, Detect, Alpn(&'static [Version]) }
```

`Detect` buffers at most 24 bytes and either matches the HTTP/2 preface or commits to h1. `Alpn` creates the engine when the handshake resolves the choice, and `poll()` yields `Event::Ready` once the application may send.

The invariant that makes this cheap: **no application bytes exist before the protocol is known.** Over TLS that holds because 0-RTT is out of scope; in cleartext it holds because `Detect` buffers 24 bytes. There is no plaintext holding pen.

---

## 9. Protocol contract

### 9.1 Events and the send API

Engines emit one vocabulary:

```rust
Event::Head { stream, head }          // ordered, duplicate-preserving fields
Event::Data { stream, body }          // Body, §7.2
Event::Trailers { stream, fields }    // terminal for the stream
Event::End { stream }                 // end of body, no trailers
Event::Reset { stream, code }         // h2 RST_STREAM / h3 RESET_STREAM; never from h1
Event::GoAway { last_stream, code }   // h2 GOAWAY / h3 close; never from h1
Event::Settings { .. }                // h2/h3 only; h1 produces none
Event::Ready                          // an engine exists and the application may send
Event::Drained                        // finish()'s output is fully produced (§6.4)
Event::Tunnel { stream }              // 101 / CONNECT established
Event::StreamWritable { stream }      // h3: the transport will take more on this stream
Event::StreamsAvailable(n)            // the peer's concurrent-stream room changed
```

The send side is its own API, because a stream must be opened before anything can be sent on it:

```rust
enum StreamKind { Request, Connect }

conn.open(kind: StreamKind) -> Result<StreamId, WouldBlock>
conn.send_head(stream, head) -> Result<()>
conn.send_data(stream, body: Body) -> Result<Flow>   // Flow::{Accepted, Blocked(Body)}
conn.send_end(stream) -> Result<()>                  // body finished, no trailers
conn.send_trailers(stream, fields) -> Result<()>
conn.reset(stream, code)
conn.goaway(last_stream, code)
conn.read_tunnel(stream, &mut ChunkMut) / conn.write_tunnel(stream, Chunk)
```

The connection's outbox, shown in §5.2, is the application's, not the driver's: `output` transfers ownership to it and `send` borrows from it, so a `WouldBlock` or a `Partial` leaves the chunk where the application can retry it. The driver holds no output queue (§6.4).

`Flow::Blocked` hands the body back rather than consuming it: a body refused by the stream's reservation is the application's to retry, and silently dropping it would be a lost body. `send_end` is separate from `send_trailers` because a body that ends plainly is not the same message as an empty trailer block, on h3 especially.

`Trailers` terminates a stream; `End` does the same without fields. That is an engine rule, not a type property — a flat event enum cannot make it unrepresentable — so the engines enforce it and the fuzz targets check it.

For TCP the driver reports connection lifecycle (`Io`). For QUIC, connections are not `ConnId`s and `h3::Endpoint::poll` reports their lifecycle instead (§9.5). The engine reports protocol facts; the transport reports socket facts.

### 9.2 Streams, admission and flow control

- **Admission.** The advertised `MAX_CONCURRENT_STREAMS` (h2) or stream limit (h3) is derived from the reservation cap divided by a minimum per-stream reservation, so the two cannot drift. Beyond it, a stream is refused before any work is done: `REFUSED_STREAM` (h2) or `H3_REQUEST_REJECTED` (h3), both of which mean *not processed, safe to retry*.
- **Reservations.** Per-stream buffered-body reservations sum to a hard cap, with a floor of at least one maximum frame per stream, so `cap/N` cannot collapse to zero for a legitimate stream.
- **Credit.** Connection-level credit returns as soon as DATA is admitted to a reservation; stream-level credit returns when the application consumes the body handle (§7.2). A stream that cannot drain within its reservation is reset — but see §11 for what code that reset carries, because "cannot drain" and "not processed" are different claims.
- **Never per-stream backpressure by stopping reads.** Stopping reads stops the whole socket (§6.2); on h3 it stops the entire endpoint. Refusal is per stream or not at all.

### 9.3 Headers

- `HeaderName` is an interned identity: registered names resolve to a `u16` at compile time, unknown names are a validated byte range compared case-insensitively. Extension headers are first-class without becoming enum variants.
- `HeaderValue` is a validated byte range with lazy typed accessors (`content_length`, `connection_tokens`, `transfer_coding`). Parsing happens when asked for, never in the parser's hot path.
- `Fields` is ordered and duplicate-preserving, with an inline representation; lookup compares the id first. No `HashMap`, no `String` allocation.
- `Head` owns its bytes: the arena (or `Chunk`) the request line and fields were parsed from, with `HeadView<'_>` reconstructed per access. That is one refcount, not one copy, and it is what lets a relay keep a head after the body chunk it arrived with is recycled.
- Wire fidelity is explicit: order and duplicates survive, unknown names and values relay byte-exactly, and the hop-by-hop strip is a method over the typed list.

### 9.4 HTTP/1.1

Hand-written single-pass parser, strict by construction: `CL`/`TE` conflicts, duplicate `Content-Length`, obs-fold, whitespace before the colon, bare CR/LF, chunk-size oddities, and any body left undrained at keep-alive are all rejected. Chunked, trailers, `100-continue`, pipelining with strictly ordered responses, HTTP/1.0 keep-alive rules. CONNECT and `101` become a `Tunnel` with half-close in both directions. The client supports buffered and streaming request bodies.

Stream ids are synthetic, assigned in pipeline order — the decision that makes a generic relay possible.

**A header that can carry trailers forces the framing decision before the head is written.** `Head` carries a `TrailerPolicy`; the relay sets `PreferChunked` whenever the source may produce trailers, drops a source `content-length`, and re-frames.

### 9.5 HTTP/2

Frames, stream states, SETTINGS, GOAWAY, RST_STREAM, flow control, HPACK (static table, Huffman, dynamic table), pseudo-header validation, forbidden connection fields, `content-length` checks, trailers. h2c prior knowledge; no `101`; plain CONNECT only.

### 9.6 HTTP/3

QUIC delegated to `quinn-proto`; framing, QPACK, control streams, SETTINGS and GOAWAY are ours.

**QPACK in v1 is format-complete with the dynamic table deferred.** v1 advertises `SETTINGS_QPACK_MAX_TABLE_CAPACITY = 0` and uses a literal-only encoder. The RFC makes that a clean cut — the default is zero, and a zero-capacity decoder's peer must not insert entries or send encoder instructions (RFC 9204 §3.2.3) — so there is no eviction, no blocked stream and no encoder-stream instruction to get wrong. Our decoder still accepts the field-section format, which is what makes raising the capacity later additive. The dynamic table has its own milestone (§14, M5).

**Egress is push-shaped.** `quinn-proto` exposes a stream write that returns `Blocked` at the stream or connection send limit and becomes writable again when flow-control credit arrives (`MAX_STREAM_DATA`/`MAX_DATA`), **not** on ACK. It cannot be handed a pre-framed connection-ordered byte stream: `output_datagram` only drains `poll_transmit`, and bytes the transport refuses wait in the engine's bounded per-stream queue until `Event::StreamWritable`. Pushing everything into quinn instead would make quinn's send buffer the real bound and blow §15's memory budget.

**Receive and credit.** `RecvStream::read(&mut self, ordered: bool)` yields `Chunks<'_>`, which borrows the connection; `Chunks::finalize()` releases the read bytes from the congestion window and returns `ShouldTransmit`. Because the value borrows, it cannot be held across the engine's own operation, so the engine copies into a pooled chunk and finalizes immediately — credit is released at read time, and the bound on a slow consumer is `TransportConfig::{receive_window, stream_receive_window}`, configured to the per-stream reservation with the sum bounded like h2's. `ShouldTransmit` is turned into output in the same `tick()`. This is the one place where the public "credit on consumption" rule (§7.2) is implemented differently under the hood, and it is stated rather than implied.

**The boundary copies, and this is the full count.** Inbound datagrams: one copy per datagram, because the transport takes `BytesMut`. Inbound body bytes: one copy out of the transport's `Chunks`, forced by this design refusing `Bytes`-backed types in the public API — not by physics, and a reader who wants it could adopt them. `poll_transmit` writes coalesced datagrams into caller-owned `Vec<u8>`, so the transport does not allocate the packet; we copy the batch into a pooled chunk once.

**Datagram metadata.** ECN is available per transmitted packet. Received ECN is an input to the transport's packet handling, not a property of an application datagram — a datagram frame can be coalesced into a packet with other frames — so the public datagram type reports no ECN. `local_ip()` is populated for servers. Nothing is fabricated.

**Not in v1.** 0-RTT, connection migration, Alt-Svc, Happy Eyeballs. The client is told to use h3, or not.

---

## 10. TLS

rustls, sans-I/O, as a layer inside `Conn`. ALPN comes from the configured policy. Certificate configuration is explicit; roots are an opt-in feature, never ambient OS state.

`read_tls` and `write_tls` move bytes through rustls' internal buffers, which is why §7.3 charges the copies it does. `send_close_notify` queues the notification and the next `write_tls` flushes it; `peer_has_closed()` detects the peer's. Half-close is the caller's: queue, flush, shut the write side down, keep reading. rustls owns no socket, which is what lets it live inside `Conn` without a thread.

TLS ships at M2, before h3, because h3 drives the same rustls through `rustls::quic` and certificate/ALPN configuration is far cheaper to debug on TCP.

---

## 11. Dispatch and workers

The application owns the threads. libhttp ships a **dispatcher**, not an executor: it owns queue admission, caps and timer order, but no thread and no polling loop — with `threads` disabled it is not in the build.

- A bounded pump→worker queue of exchanges.
- A bounded worker→pump queue for responses and body requests, with **`WakeHandle`** — one per shard, `Send + Sync`, `wake()` only — so a worker never touches a `&mut Driver`.
- Topology: one shared queue and one `WakeHandle` per shard. A worker holding an exchange from shard S wakes S and only S.
- Caps: **8 parked workers per connection** and **64 parked globally** as first-cut tunable constants. A per-connection cap alone bounds nothing — 100 connections × 8 parked handlers exhausts a 64-thread pool.
- When a cap is hit, the stream is refused before any work is done, with the not-processed code (§9.2). Once work has been dispatched, a deadline does **not** carry a retryable code: the peer must not replay side effects. h2 gets `CANCEL` (or a 504 if the head was already sent), h3 gets `H3_REQUEST_CANCELLED`, h1 gets a 504 or a close. This is the difference between "we refused it" and "it may have run".
- A worker parks without a clock; the pump owns time and wakes it with a typed reason. After a reset, every `Exchange`, reader and writer returns `Err(Reset)` — a contract, because a late worker must fail rather than write into returned buffers. A worker blocked inside its own database cannot be cancelled by anyone; that is stated, not papered over.

The pump runs no application code. That invariant is what makes every timing claim in §6 true, and `Server` asserts it in debug builds.

---

## 12. Convenience layer

`Server` and `Client` exist so a normal application does not write the pump loop, and they are built against exactly the public API. They own the threads they spawn and live behind `threads`. `Handler` is an ordinary function taking `&mut Exchange` — typed head, streaming body, streaming response, trailers. No `Body` trait, no `Service`, no per-request boxing.

If `Server` needs something the public API lacks, the API is wrong and gets fixed. That is the price of the proxy being the acid test.

Name resolution is not in the core: `Driver::connect` takes a `SocketAddr`. The convenience layer resolves once per connection on a thread it owns, uses the first address, and does not race — Happy Eyeballs is deferred, and saying so is better than half-supporting it. The proxy's only worker involvement is that resolution (§13).

---

## 13. Proxy

A state machine on the pump thread, built only from public types, that never blocks.

- Six protocols pairs: h1↔h1, h1↔h2, h2↔h2, h1↔h3, h2↔h3, h3↔h3. The only per-protocol code is connection setup, because the event vocabulary is shared.
- Bodies relay as `Body` handles: no copy, one refcount per split, and the credit travels with the handle to the destination stream.
- Hop-by-hop fields are stripped by typed list.
- **The h1 leg is connection-granular**, because the protocol is: no per-stream window, no per-stream abort, and stopping reads stops the socket. So the relay never multiplexes concurrent streaming exchanges onto one h1 connection; it carries one streaming exchange at a time and is pooled for head-complete ones. An h2 or h3 leg is stream-granular, with the reservations and credit of §9.2.
- A per-stream failure on an h1 leg closes the leg and fails every stream on it with a gateway error, since pretending h1 can abort one response mid-body is not an option that exists.
- CONNECT and `101` become `Tunnel` pairs; half-close forwards in both directions via §6.4.

The v1 proxy is single-driver-thread per instance. Multi-shard relaying uses the same primitives (`Body` crossing shards, batched returns) and is not in the gate.

---

## 14. Milestones and gates

**M1 — transport and HTTP/1.1.** Driver on all three platforms (A path), chunks and pools, the h1 engine, `Conn`, h1↔h1 proxy, parser fuzz targets. No TLS, no h2, no h3.

**M2 — TLS and HTTP/2.** rustls in `Conn`; h2 frames, HPACK, flow control, credit; h2 client and server; proxy h1↔h2 and h2↔h2.

**M3 — HTTP/3.** quinn-proto behind the datagram edge, our framing and QPACK (capacity zero, literal-only encoder), h3 client and server, the remaining relay pairs, interop against an independent implementation run either as a dev-dependency or as a separate process.

**M4 — accelerators.** Each lands separately, behind its own gate, where it applies: io_uring submission batching for datagrams and file bodies, `SEND_ZC`, splice, `sendfile`, `TransmitFile`, batched UDP. `send` stays a synchronous non-blocking syscall on every backend (§4). A path that does not beat the portable path on the published harness stays off by default.

**M5 — QPACK dynamic table.** Encoder-side eviction safety, decoder-side blocked streams, and a compression benchmark that has to show the win. Additive by construction: same wire format, same settings, no public change.

### Gate policy

A milestone is an exit condition, not a date: the functional suite passes **and** the gate measures. Correctness blocks unconditionally. Performance blocks by default and can be waived in writing with the measured gap and a follow-up item; it is never closed quietly unmeasured.

**Gates 1 (M1) and 2 (M2)** compare against hyper on the same host, same rustc and kernel, over four endpoints — a trivial GET, a pipelined GET pair, a 1 MiB body through the proxy, and a streaming upload through a worker — plus one combined load of 10k idle keep-alives with a single hot connection. Recorded: throughput, p50, p99, syscalls, allocations, copies.

Pass, on the platforms the gate covers: throughput ≥ 95% of hyper, p99 ≤ 110%, copies and allocations no worse, all six numbers published with the harness.

**Gate 3 (M3)** is correctness-gated: interop against an independent implementation. There is no hyper h3, so its numbers are published as published, not compared to a hyper margin.

**Gate 4** is per accelerator, against the portable path on the same host. **Gate 5** is the compression benchmark.

The memory budget the gates report, as targets rather than claims:

| State | Target | Dominant term |
|---|---|---|
| Idle h1 keep-alive | ≤ 5 KiB | the 4 KiB chunk a partial head sits in, plus parser state |
| Idle h2 keep-alive | ≤ 12 KiB | two HPACK tables, 4 KiB each (encoder and decoder), plus stream state |
| Idle h3 | ≤ 32 KiB | transport loss-recovery and crypto state |
| TLS, on top | 48 KiB configured (16 + 16 + 16) | `received_plaintext` 16 KiB is **outside** `set_buffer_limit`; the two sendable buffers are 16 KiB each once configured, 64 KiB each by default |
| Chunk pool, per shard | ≤ 64 MiB cap | RSS is cap × shards, and it is the largest single line item at 8 shards |

At 10k connections this is ~50 MiB (h1), ~120 MiB (h2), ~320 MiB (h3) before TLS, and ~480 MiB with TLS at the configured limits. The gate publishes TLS-on and TLS-off separately, because a blended number hides the largest term.

---

## 15. Crate and dependencies

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
| `rustls` | Sans-I/O TLS; the only TLS stack considered that brings no runtime |
| `ring` | rustls' crypto provider. See the deviation in §16 |
| `webpki-roots` | Client roots, opt-in |
| `quinn-proto` 0.11 | Sans-I/O QUIC transport; pure Rust, no runtime, MSRV 1.85 |
| `libc` | Unix syscalls |
| `windows-sys` | IOCP, AFD ioctls, socket options |
| `io-uring` | Phase 2 Linux accelerator only |

`rustls` and `quinn-proto` are configured deliberately because their defaults contradict this design: rustls defaults to aws-lc-rs, quinn-proto's default features pull both providers plus `rustls-platform-verifier` (ambient OS roots). We pin `ring`, disable quinn's defaults, and make roots explicit.

Dev-only, never in the product: hyper (the baseline), an independent h2/h3 implementation (interop), a fuzz harness.

Rejected QUIC transports: quiche (BoringSSL + cmake), s2n-quic (mandatory tokio), neqo (NSS), gm-quic (async-bound).

---

## 16. Open decision

**The C toolchain.** The brief says TLS must not force a C toolchain unless the QUIC decision requires it. `ring` compiles C and assembly, and rustls has no mature pure-Rust provider, so the current choice is a **deviation from the brief**, not an interpretation of it. The alternatives are aws-lc-rs (also native, plus CMake and NASM) or an experimental provider — neither is a smaller deviation. Recommendation: accept it, record it here as a deviation with the build requirement in the README. This is the only decision in the document still open.

---

## 17. Platform risk

The Windows backend depends on an internal NT interface. That is the one platform decision that is not purely a performance decision.

- A startup self-test verifies AFD *semantics* — read, write, abort, and re-reporting after an event was deliberately not drained — not merely that the handle opens. Any mismatch falls back automatically.
- Every gate number records which Windows path produced it.
- Gate 1's Windows entry is scoped to AFD-present. The fallback gets its own pass condition measuring O(n) poll cost honestly rather than being compared to hyper, because hyper on Windows reaches AFD through mio.

If AFD disappears, correctness still holds and the Windows performance claim does not. Saying that before the gate is cheaper than discovering it at one.

---

## 18. Verification

- Fuzz targets: h1 parser, h2 frame/HPACK decoder, h3/QPACK decoder, all reachable from a byte slice with no I/O.
- Conformance: an h1 smuggling corpus; h2 and h3 interop against an independent implementation, as client and as server.
- Portability: the functional suite runs on Linux, macOS and Windows on every change; numbers are per platform and never extrapolated.
- The harness is published with the numbers, including syscalls, allocations and copies, so a bad result is reproducible rather than argued about.

---

## Appendix A — Where each §10 question is answered

| # | Question | Answer |
|---|---|---|
| 1 | I/O model across platforms | Readiness, level-triggered; completions for submission only. §4, §6.1, §6.8 |
| 2 | Arming, backpressure, close, drain | Interest and pause; `activate`; `close`/`finish`/`Drained`. §6.2, §6.4 |
| 3 | Buffer ownership and budget | One atomic refcount, chunk chains, one credit handle. §7 |
| 4 | Worker model | Dispatch with `WakeHandle`s, two caps, pump-owned time. §11 |
| 5 | Fast-path scope | v1 is the portable path; M4 per accelerator, each gated. §14 |
| 6 | QUIC backend and budget | quinn-proto; copies counted; QPACK capacity zero. §9.6 |
| 7 | TLS integration and order | rustls inside `Conn`; TLS before h3. §10, §14 |
| 8 | File bodies | M4 op, app-supplied handle, never on the pump thread. §14 |
| 9 | Half-close and tunnels | `shutdown_write` after `close_notify`; `Tunnel`. §6.4, §13 |
| 10 | Socket ownership | Driver tables, generational ids, no adoption. §6.3 |
| 11 | Performance gate | M1/M2 vs hyper; M3 correctness; M4 per path. §14 |
| 12 | Crate, features, MSRV | One crate, five features, 1.85. §15 |

---

## Appendix B — Platform facts relied on

Checked 2026-10-26 against primary sources. These are the facts that would change the design if wrong.

| Fact | Source |
|---|---|
| kqueue is level-triggered; `EV_CLEAR` is the edge mode | [FreeBSD kqueue(2)](https://man.freebsd.org/cgi/man.cgi?query=kqueue&sektion=2) |
| `sendfile` on macOS and FreeBSD takes `sf_hdtr`, so headers and trailers need no extra `writev` | [Apple](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/sendfile.2.html), [FreeBSD](https://man.freebsd.org/cgi/man.cgi?query=sendfile&sektion=2) |
| `WSAPoll` marshals the array per call and never reports a failed connect | [Microsoft](https://learn.microsoft.com/en-us/windows/win32/api/winsock2/nf-winsock2-wsapoll), [curl](https://daniel.haxx.se/blog/2012/10/10/wsapoll-is-broken/) |
| A zero-byte overlapped `WSASend` completes immediately with `SO_SNDBUF` full, so write readiness cannot be probed | empirical, treated as a constraint ([source](https://stackoverflow.com/posts/42019668/timeline)) |
| mio 1.x and wepoll get readiness from `IOCTL_AFD_POLL` on `\Device\Afd`, completions via IOCP | [mio afd.rs](https://docs.rs/crate/mio/1.2.4/source/src/sys/windows/afd.rs) |
| `GetQueuedCompletionStatusEx` returns a batch in one call | [Microsoft](https://learn.microsoft.com/en-us/windows/win32/fileio/getqueuedcompletionstatusex-func) |
| `SEND_ZC` produces two CQEs; the `F_NOTIF` one means "buffer reusable", and errored requests still notify | [io_uring_enter(2)](https://manpages.debian.org/unstable/liburing-dev/io_uring_enter.2.en.html) |
| Multishot recv needs provided buffers; a per-call buffer fails `EINVAL` | [liburing 1331](https://github.com/axboe/liburing/issues/1331) |
| Emulated readiness can be spurious; callers must handle `EAGAIN` | [libuv poll](https://docs.libuv.org/en/v1.x/poll.html) |

## Appendix C — QUIC and TLS facts relied on

| Fact | Source |
|---|---|
| `RecvStream::read(&mut self, ordered: bool) -> Result<Chunks<'_>, ReadableError>`; `Chunks` borrows the connection | [docs.rs](https://docs.rs/quinn-proto/0.11.19/quinn_proto/struct.RecvStream.html) |
| `Chunks::finalize(self) -> ShouldTransmit` releases the congestion window; dropping also releases; leaking keeps the peer blocked | [docs.rs](https://docs.rs/quinn-proto/0.11.19/quinn_proto/struct.Chunks.html) |
| `WriteError::Blocked` clears on flow-control credit, not on ACK | [docs.rs](https://docs.rs/quinn-proto/0.11.19/quinn_proto/enum.WriteError.html) |
| `poll_transmit(now, max_datagrams, &mut Vec<u8>)` writes egress into caller storage | [docs.rs](https://docs.rs/quinn-proto/0.11.19/quinn_proto/struct.Connection.html) |
| `Datagrams::send(Bytes)` / `recv() -> Option<Bytes>`; no ECN on that surface | [docs.rs](https://docs.rs/quinn-proto/0.11.19/quinn_proto/struct.Datagrams.html) |
| rustls: `set_buffer_limit` covers only `sendable_plaintext` and `sendable_tls`; `received_plaintext` is a fixed 16 KiB; the default limit is 64 KiB | vendored `conn.rs` 543–545, `cs.rs` 87, 1135–1136 |
| `read_tls`/`write_tls` copy through internal buffers; `send_close_notify` queues and `write_tls` flushes | [rustls ConnectionCommon](https://docs.rs/rustls/0.23.46/rustls/struct.ConnectionCommon.html) |
| QPACK capacity defaults to zero; a zero-capacity decoder's peer must not insert or send encoder instructions | [RFC 9204 §3.2.3](https://www.rfc-editor.org/rfc/rfc9204.html) |
| quiche needs BoringSSL and cmake; s2n-quic mandates tokio; neqo needs NSS; gm-quic is async-bound | [quiche](https://raw.githubusercontent.com/cloudflare/quiche/master/README.md), [s2n-quic](https://crates.io/api/v1/crates/s2n-quic/1.91.0), [neqo](https://raw.githubusercontent.com/mozilla/neqo/main/README.md), [gm-quic](https://crates.io/api/v1/crates/gm-quic/0.4.0/dependencies) |
