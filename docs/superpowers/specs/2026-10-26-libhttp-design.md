# libhttp — Design

**Status:** draft for review.
**Date:** 2026-10-26.
**Companion:** `2026-10-26-libhttp-requirements.md` (the brief). This document was written from that brief alone; the archived revisions were not read.
**What this is:** the architecture, the public contracts, and the reasoning behind them. It is written to be planned from without going back to the brief.

---

## 1. Intent

libhttp is a Rust HTTP library — not a framework, not a runtime. It exists because the available stacks force an abstraction (futures, a body trait, a service tower), hide copies and wakeups, and assume an async runtime, which makes performance impossible to reason about.

The design bet is that **explicitness and speed are the same thing here**: if every copy, allocation, wakeup and syscall has a name and an owner, the fast path is not a special case, it is the obvious one. Everything below follows from that.

Three sentences that describe the whole library:

- The application owns its threads and its event loop; libhttp owns sockets, buffers, timers and protocol state.
- The public I/O contract is readiness; completion-based I/O exists only underneath it, where a benchmark proves it earns its place.
- Protocol engines are sans-I/O byte machines with one shared event vocabulary, so an HTTP/1.1↔HTTP/3 relay is the same code as HTTP/2↔HTTP/2.

---

## 2. Goals, non-goals, anti-goals

### Goals

- HTTP/1.1, HTTP/2 and HTTP/3, client and server, over TLS, with a reverse proxy built from the same public pieces.
- Synchronous application code. No `Future`, `Waker`, executor, or runtime anywhere, including inside TLS and QUIC.
- No thread per connection, no blocking socket I/O on an I/O thread, no hidden threads in the core.
- Identical public API on Linux, macOS/BSD and Windows. Platform acceleration changes cost, never behaviour.
- A low-level API complete enough that the shipped proxy needs nothing private.
- Bounded memory with a stated budget, and a copy/atomic/syscall accounting that a reader can check.

### Non-goals

- A web framework. No routing, no extractors, no middleware tower.
- A tokio compatibility layer, or integration with any async runtime.
- HTTP/2 server push, HTTP/2 priority, HTTP/3 connection migration, 0-RTT.
- A general-purpose I/O library. It speaks HTTP; the reactor is not a public async I/O facility for other protocols.

### Anti-goals

Things that would make this a failure even if the tests pass:

- **A fast path that hides work.** Batching that silently buffers, a pool that silently allocates, a wakeup that silently crosses threads. If it cannot be named in the costs table, it does not ship.
- **Two APIs for the same thing** — a "simple" path and a "real" path that behave differently. There is one path, and a convenience layer that is written in terms of it.
- **Platform-shaped public types.** No `fd`, no `epoll`, no `io_uring`, no `IoSlice` in a signature.
- **Correctness traded for a number.** A benchmark-gated fast path that changes observable behaviour is not a fast path.
- **A proxy that needs an escape hatch.** If the proxy reaches for a private module, the low-level API failed and the design is wrong.

---

## 3. Constraints

Hard, from the brief:

- No `async`/`.await`, no `Future`, `Waker`, or hidden executor — not for TLS, not for QUIC, not internally.
- No blocking socket I/O on an I/O thread; no thread per connection.
- Linux, macOS/BSD and Windows are first-class; CI builds and runs functional tests on all three; numbers are published per platform.
- Typed protocol model: `Method`, `StatusCode`, `Version`, typed header names and values, parsed targets. No `String`/`HashMap<String, String>` on the hot path.
- No `bytes`-style buffer implementation; no `Bytes`/`BytesMut` core. No `httparse`. No `hyper`, `h2`, `h3`, `tokio`.
- OS crates (`libc`, `windows-sys`, `io-uring`) are fine. rustls for TLS. QUIC transport may be delegated; HTTP/3 framing and QPACK are ours.
- Dependencies must prove measured value.

Additional constraints that fall out of the architecture and are not negotiable:

- **The core spawns no threads.** The only code that spawns is the convenience layer, and it is a thin wrapper over the public API.
- **The driver is the single owner of timers.** If a deadline can be missed by calling `wait` with the wrong timeout, the API is wrong.
- **Everything the proxy needs is public.** The proxy is the acceptance test for the low-level layer.

---

## 4. Approaches considered

### 4.1 Who owns the threads

| Option | Consequence |
|---|---|
| Library owns reactor threads and a handler pool behind a handle you start | Smallest application code; but a hidden executor in all but name, and the brief's "no hidden executor" would be satisfied only on a technicality |
| **Application owns the threads and pumps a driver** | Chosen. No hidden control flow at all; the library cannot be blamed for scheduling decisions it did not make |

### 4.2 Where the public boundary sits

| Option | Consequence |
|---|---|
| Streams and exchanges (ask for the next head, get a reader/writer) | Smallest application surface; but hides exactly the flow-control, reset and half-close mechanics the proxy must drive, so those become library policy rather than application decisions |
| **Packets and semantic events** | Chosen. The application sees socket bytes and protocol events; h1/h2/h3 share one event vocabulary, so cross-protocol relay is free |

### 4.3 The I/O contract

| Option | Consequence |
|---|---|
| **A. Readiness only, portable core** | One non-blocking `recv`/`send` after one wakeup, everywhere. Simplest to verify, honest parity, gives up batching |
| **B. Readiness contract, completion backend where it pays** | Chosen, with A as the shipping base. Same public contract; io_uring/IOCP used underneath for writes, zero-copy send, file bodies, datagrams — each behind a benchmark gate |
| C. Per-platform native public API | Rejected: three public I/O models, violating the uniform-API constraint |

The direction of that last decision is asymmetric and worth stating: a readiness contract can be implemented on completions without the caller noticing (`recv` returns data or `WouldBlock` either way). A completion contract cannot be implemented on readiness without a helper thread and an operation lifecycle the caller must manage. Readiness is the honest lowest common denominator; completions are an optimisation.

---

## 5. Architecture

### 5.1 Layers

```
  application (threads, pump loop, handlers, the proxy)
  ─────────────────────────────────────────────────────────
  convenience    Server / Client / Workers         thin, spawns, feature-gated
  ─────────────────────────────────────────────────────────
  connection     Conn: TLS + protocol engine       feed / poll / output
  ─────────────────────────────────────────────────────────
  protocol       h1 / h2 / h3 engines              sans-I/O, shared events
  ─────────────────────────────────────────────────────────
  transport      Driver: sockets, readiness, timers, datagrams
  ─────────────────────────────────────────────────────────
  platform       epoll · kqueue · AFD/IOCP   (+ io_uring as an accelerator)
```

The dependency arrow points down only. The transport layer does not know what HTTP is; the protocol layer does not know what a socket is.

### 5.2 The public model, in one page

```rust
// Transport. The application calls these; nothing here blocks.
let mut drv = Driver::new(Config::default())?;
let listener = drv.listen("0.0.0.0:8080".parse()?)?;

loop {
    drv.wait()?;                       // sleeps until an event or the next timer

    while let Some(io) = drv.next_io() {
        match io {
            Io::Acceptable(l) => {
                let conn = drv.accept(l)?;         // ConnId, socket already non-blocking
                // application creates a connection object for `conn`
            }
            Io::Readable(c) => {
                // level-triggered: drain what is there, or leave it and get the event again
                while let Ok(mut chunk) = pool.get(READ_SIZE) {
                    match drv.recv(c, &mut chunk)? {
                        Read::Data(n) => {
                            chunk.truncate(n);
                            connections[c].feed(chunk)?;   // TLS decrypt + parse, may retain spans
                        }
                        Read::WouldBlock | Read::Eof => break,
                    }
                }
                // `Err` from pool.get means the buffer budget is spent: stop reading.
                // Readiness repeats, so the retry needs no wakeup of its own.
            }
            Io::Writable(c) => { /* flush what the connection has produced */ }
            Io::Timer(t)    => { /* route to the connection that armed it */ }
            Io::Closed(c)   => { connections.remove(c); }
            Io::Datagram(u) => { /* h3: feed a QUIC datagram */ }
        }
    }

    for (id, conn) in connections.iter_mut() {
        while let Some(ev) = conn.poll() { route(ev) }        // semantic protocol events
        while let Some(out) = conn.output() { drv.send(*id, out)?; }
        conn.arm(&mut drv);                                    // its deadlines go in the driver heap
    }
}
```

A connection object is the TLS+protocol stack:

```rust
conn.feed(chunk) -> Result<()>        // ciphertext in; decrypts, parses, retains refcounted spans
conn.poll() -> Option<Event>          // Head, Data, Trailers, End, Reset, GoAway, Settings, Tunnel
conn.output() -> Option<Chunk>        // already framed and encrypted, ready for the socket
conn.finish()                         // queue the graceful shutdown (close_notify, GOAWAY)
```

The application writes the loop. Everything the loop touches is public.

---

## 6. Transport: the `Driver`

### 6.1 Readiness contract

Readiness is **level-triggered on every platform**. `wait` re-reports a socket that still has data pending, so an application that ignores an event is slow, not broken, and there is no edge to lose. Level triggering is also what makes `recv`-after-`wait` safe to write: the caller never needs a "drain until `WouldBlock` or else the connection hangs" rule, though draining is still the efficient thing to do.

`recv` and `send` are non-blocking by contract and return `WouldBlock`. The driver never parks the calling thread; the application's loop does all the waiting, once, in `wait`.

There is no re-arm step. A socket reports readiness while it has readiness to report. This is the whole arming story, and it is deliberately boring.

Readiness is a hint, not a promise, and the contract says so out loud: a `recv` after a read event may still return `WouldBlock`. That is not a defect to paper over — every readiness layer produces spurious wakeups, and the Windows one does so by construction (§6.7). Code that treats "readable" as "bytes are waiting" is wrong on every platform; code that loops until `WouldBlock` is right on every platform.

### 6.2 Interest and backpressure

`drv.interest(conn, Interest::READ | Interest::WRITE)` controls what is reported. Read interest is on from accept/connect. Write interest is turned on when the application has output that `send` could not take, and off again when it drains — so a quiet connection generates no write wakeups.

Backpressure is the absence of calls: an application that stops calling `recv` stops reading, and the peer's window closes because the bytes are not consumed. There is no separate pause state to get out of sync.

### 6.3 Timers

The driver owns one timer heap. Connections do not expose deadlines for the application to aggregate — that would make "missed deadline" a user error. Instead `conn.arm(&mut drv)` inserts or updates that connection's timers in the driver's heap, and `drv.wait()` derives its own timeout from the heap. The earliest timer always fires; no call site can pass a wrong timeout.

Timer events carry a generational `TimerId`. A timer that was disarmed or re-armed between the wait and the drain is recognised as stale and dropped. Deadlines therefore have exactly one owner, and there is no race between a worker timeout and a reactor close: the pump thread fires the deadline and *it* is what cancels the parked work.

### 6.4 Sockets, ids and ownership

The driver owns an authoritative, mutable table of listeners, connected sockets and UDP sockets. `ConnId`, `ListenerId` and `UdpId` are generational handles; using a stale one is an error, never a use-after-free.

The application never sees a file descriptor. It cannot register its own socket either: `listen` and `connect` create them, `accept` produces a `ConnId`. There is no socket-adoption path in v1, because adopting is where a portable API starts leaking `fd`s.

Closing is two operations, because graceful and abrupt are different things:

- `drv.close(conn)` — abort. The socket is removed, retained chunks are dropped, one `Closed` event is delivered.
- `drv.finish(conn)` — graceful. The connection is asked for its final output (`close_notify`, h2 GOAWAY), then the application writes it and calls `drv.shutdown_write(conn)`. The socket stays readable until the peer closes or its deadline fires.

There is nothing for the driver to drain on close that the application still holds, because read buffers are never handed out and then forgotten: the application's own call is the only thing in flight.

### 6.5 Half-close and tunnels

`shutdown_write(conn)` maps to `shutdown(SHUT_WR)` on Unix and `shutdown(SD_SEND)` on Windows, after the TLS layer has emitted `close_notify`. This is required for CONNECT and `101` tunnels and is not an optimisation.

`Tunnel` is the only place raw bytes are public. After a `101` or a successful CONNECT, the connection yields `Event::Tunnel(stream)`, and `conn.read_tunnel(..)`/`conn.write_tunnel(..)` move opaque bytes until either side half-closes. That is exactly where HTTP stops being HTTP, so it is exactly where bytes should surface.

### 6.6 Datagrams

UDP sockets are first-class in the driver from M1, not bolted on for h3: `recv_datagrams(&mut [Datagram])` and `send_datagrams(&[Datagram])` take batches, and each `Datagram` carries its source/destination address and, where the platform provides it, ECN. The library's config states which metadata is genuinely available per platform and which is reported as `None`; it never fabricates it.

The readiness contract covers datagrams unchanged: readability means at least one datagram is available, and the batch read drains what is there.

### 6.7 Windows: AFD polling, not completion probes

The obvious Windows implementation — IOCP plus a zero-byte overlapped `WSARecv` as a readiness probe — works for reads and fails for writes. A zero-byte `WSASend` completes immediately even when `SO_SNDBUF` is full, so write readiness cannot be probed that way, and a contract that cannot report write readiness is not a readiness contract.

So the Windows backend polls with `IOCTL_AFD_POLL` against `\Device\Afd`, which is the interface mio 1.x and wepoll use. It reports receive, send, accept, connect and abort readiness for sockets and datagrams, and its completions arrive through an IOCP, so batched completion retrieval (`GetQueuedCompletionStatusEx`) still applies. It is an internal NT interface. That is a named risk, not a hidden one: it is battle-tested by the two most widely deployed Rust and C portability layers, but it is not a documented contract, and §17 records what happens if it breaks.

The documented fallback is `WSAPoll` for read and write readiness, with `ConnectEx` completions for connect, because `WSAPoll` never reports a failed connect — a known, won't-fix bug. It is O(n) per call and slower, it is correct, and it is what runs if the AFD handle cannot be opened.

### 6.8 The `Io` vocabulary

```rust
enum Io {
    Acceptable(ListenerId),
    Connected(ConnId),                      // non-blocking connect finished
    ConnectFailed(ConnId, ConnectError),    // refused, unreachable, timed out
    Readable(ConnId),
    Writable(ConnId),
    Closed(ConnId, CloseReason),            // exactly once, always the last event for that id
    Datagram(UdpId),
    Timer(TimerId),
}

enum CloseReason {
    Local,               // we called close or finish
    Peer,                // clean EOF; with TLS, close_notify was seen
    PeerReset,           // RST, or a QUIC reset
    TimedOut,            // a deadline fired and the driver closed it
    Tls(TlsError),       // alert, or a truncated close — an error, never a clean EOF
    Io(IoError),
}
```

`CloseReason` is what turns the proxy's 502 path and the client's retry decision into a match instead of a guess. `Closed` is terminal: after it the id is stale and using it is an error, never a panic.

---

## 7. Chunks

`Chunk` is a handle to a pooled allocation: storage, a range, a refcount and the pool it came from.

- `Chunk::clone` and `Chunk::slice(range)` share storage and bump one atomic. A chunk splits without copying.
- `Chunk` is `Send`. It is not `Sync`; mutating requires exclusive access, which the type system enforces.
- Dropping the last reference returns the storage to the owning pool. If the owning thread is not the current one, the return is batched into that pool's inbound queue and one wakeup is issued per batch — never one wakeup per buffer.
- Writing requires an owned chunk. A received chunk is immutable; a split view is immutable. Mutability is not smuggled in through an interior handle.
- Chunks carry a fixed headroom (16 bytes) so a protocol writer can back-fill a frame length without shifting bytes. This exists because QPACK and h2 both need to prepend a length after the payload is encoded.

`ChunkPool` is per reactor shard, size-classed (2 KiB / 16 KiB / 64 KiB in the first cut), and bounded. `pool.get(size)` takes from the class or allocates; the allocation is counted and visible in metrics. The pool never grows without bound, and the sizes are a builder-tunable constant, not a hidden policy.

### When the pool is full

`pool.get` returns `Result<Chunk, Exhausted>` and fails at the configured byte cap. It does not allocate past the cap and it does not block. The documented response is to stop reading: the socket keeps its bytes, the peer's window closes, and level-triggered readiness re-reports when the application can read again. Buffer exhaustion is therefore a self-healing backpressure signal, not an error path — which is why the pump loop in §5.2 can treat `Err` as "break" and still be correct.

### How stream credit gets returned

h2 and h3 stream-level flow control returns credit when the application has *consumed* a body chunk, not when it arrives; that is what stops one slow stream from starving its neighbours. The mechanism has to be free:

- A `Data` event's chunk carries a 16-byte credit token in its chunk header: owning connection, stream, byte count.
- When the last reference to that chunk drops, the pool pushes the token onto the owning connection's preallocated credit queue.
- `conn.tick()` drains the queue and hands the credit back to the engine.

No allocation, no per-event callback, no atomic beyond the refcount that already exists. A split view shares one token, so a body split across two relay legs or two streams returns its credit exactly once, when the last view is gone — which is the correct accounting, because the peer's window is about bytes not yet consumed anywhere.

Connection-level credit is the opposite and does not wait: it is returned as soon as DATA is admitted to the connection's bounded reservation (§9.3), because otherwise one unread stream would freeze the whole connection's window.

### The copy and atomic budget

| Path | Copies | Atomics |
|---|---|---|
| Plaintext body, socket → engine | 1 (kernel → chunk) | refcount only if the chunk is split |
| Plaintext body, engine → socket | 1 (chunk → kernel) | none |
| TLS inbound | + up to 2 (chunk → rustls buffer → plaintext chunk) | none |
| TLS outbound | + 1 (plaintext → rustls buffer → ciphertext chunk) | none |
| h3 inbound datagram | + 1 (chunk → `BytesMut`) | none |
| h3 outbound transmit batch | + 1 (`Vec` → chunk), once per batch | none |
| Proxy body relay, same shard | 0 | 1 refcount bump per split |
| Proxy body relay, cross shard | 0, plus one batched return | 2 refcount bumps, one batched wakeup |
| h2/h3 data split across streams | 0 | 1 refcount bump per stream |

The TLS numbers are not an accident and not hidden: they are the cost of rustls' sans-I/O API, they are confirmed against the 0.23 surface and measured at M2, and an in-place deframer is a named follow-up if the number is bad. The proxy body path is the one the brief cares about most, and it either copies zero times or this design is wrong.

---

## 8. The connection object

A `Conn` is TLS plus one protocol engine, driven by three calls: `feed`, `poll`, `output`.

- `feed` takes ciphertext (or a datagram, for h3), runs the TLS layer if present, and hands plaintext to the engine. The engine parses in place and retains slices as needed.
- `poll` yields semantic events. It never yields frames, HPACK/QPACK instructions, or partial parse state.
- `output` yields framed, encrypted bytes in connection order, ready for `send`. It returns nothing while flow control forbids sending; the engine queues up to its reservation and no further.

TLS is a layer, not a second pump. The application never sees plaintext/ciphertext plumbing; it sees `feed` and `output`, and a config that says whether TLS is on. This is what keeps "just enough" true for both the simple server and the relay.

Adding h3 changes the input type, not the shape: `feed_datagram` and `output_datagram` instead of `feed` and `output`. Events are identical.

### 8.1 Choosing the protocol

A connection is created with a policy, not an engine:

```rust
enum Protocol {
    Http1,                    // h1 only
    Http2,                    // h2c prior knowledge, no upgrade dance
    Detect,                   // cleartext: HTTP/2 preface or h1 request line
    Alpn(&'static [Version]), // TLS: the offered list; the engine follows the choice
}
```

`Conn::new(id, protocol, tls_config)` starts in a handshaking state with no engine. `poll()` yields `Event::Ready` exactly once, when an engine exists and the application may send; `conn.protocol()` reports what was chosen.

The invariant that makes this cheap: **no application bytes exist before the protocol is known.** Over TLS that holds because 0-RTT is out of scope — the server picks ALPN in the ServerHello, and the client sends no application data until the handshake completes. In cleartext, `Detect` buffers at most 24 bytes and either matches the HTTP/2 preface or commits to h1. So there is no case where plaintext arrives with no engine to hand it to, which is why `Conn` needs no plaintext holding pen.

Rejected: one protocol per listener. It forces two ports for a server that speaks h1 and h2, and it makes the proxy's upstream configuration guess the peer's protocol instead of negotiating it.

---

## 9. Protocol engines

### 9.1 One event vocabulary

All engines emit and accept the same events:

```rust
Event::Head { stream, head }          // method, target, version, ordered fields
Event::Data { stream, chunk }         // body bytes, refcounted
Event::Trailers { stream, fields }
Event::End { stream }                 // end of body
Event::Reset { stream, code }         // h2 RST_STREAM / h3 RESET_STREAM; never from h1
Event::GoAway { last_stream, code }   // h2 GOAWAY / h3 close; never from h1
Event::Settings { .. }                // peer settings; h1 synthesises defaults
Event::Ready                          // an engine exists and the application may send
Event::Tunnel { stream }              // 101 / CONNECT established
```

Connection lifecycle — connected, closed, failed — is reported by the driver (`Io`), not by the engine. The engine reports protocol facts; the driver reports socket facts. Keeping those apart is what lets the same engine sit over TLS, plain TCP or QUIC without knowing which it is on.

HTTP/1.1 has no stream ids, so the engine assigns synthetic ones in pipeline order. That single decision is what makes a generic relay possible: the proxy does not branch on protocol version when moving a body, only when configuring the connection.

### 9.2 HTTP/1.1

- Hand-written single-pass parser. Strict by construction: rejected `CL`/`TE` conflicts, duplicate `Content-Length`, obs-fold, whitespace before the colon, bare CR/LF, chunk-size oddities, and any body that would be left undrained at keep-alive.
- Chunked, trailers, `100-continue`, pipelining with strictly ordered responses, HTTP/1.0 keep-alive rules.
- CONNECT and `101` become a `Tunnel`; half-close is supported on both directions.
- Client: buffered and streaming request bodies; streaming responses.

### 9.3 HTTP/2

- Frames, stream states, SETTINGS, GOAWAY, RST_STREAM, flow control, HPACK (static table, Huffman, dynamic table) implemented here.
- Pseudo-header validation, forbidden connection fields, `content-length` checking, trailers.
- h2c prior knowledge. No `101`. Plain CONNECT only; extended CONNECT is out of scope.
- **Credit policy:** connection-level window is returned as soon as DATA is admitted into the connection's bounded per-stream reservations; stream-level window is returned only when the application consumes the chunk (drops it). This is what stops one slow stream from starving the others, and it needs the per-stream reservations to sum to a hard cap; a stream that cannot drain within its reservation is reset and its connection credit returned.
- A parked worker cannot hold a stream's window open indefinitely: the reservation is the bound, and the deadline is what closes it.

### 9.4 HTTP/3

- QUIC transport delegated to `quinn-proto`; framing, QPACK, control/request stream handling, SETTINGS and GOAWAY are ours.
- Three control streams (control, encoder, decoder) each with their own ordered output, merged into connection order by the engine. This is why `output` is per connection, not per stream.
- QPACK: the RFC 9204 static table (99 entries), Huffman (shared with HPACK), the dynamic table with encoder/decoder instructions, and a blocked-stream bound from `SETTINGS_QPACK_BLOCKED_STREAMS`.
- **The QUIC boundary copies, and this is where.** Inbound: `quinn-proto` accepts packets as `BytesMut`, so every received datagram is copied once out of its chunk; there is no slice-in path. Outbound: `poll_transmit` writes coalesced datagrams into caller-owned `Vec<u8>` storage, so the transport does not allocate the packet — we copy that batch into a pooled chunk once, for the whole batch rather than per datagram. RFC 9221 datagrams travel through `Datagrams::send`/`recv`, which take and yield `Bytes`, so they cost one copy each way.
- Metadata: ECN is available per transmitted packet (`Transmit::ecn`) and per received packet, but *not* on the RFC 9221 datagram surface, and `local_ip()` is only populated for servers. The public datagram type reports what is actually there and `None` otherwise; it never fabricates metadata. That is a limit of the chosen transport, stated rather than papered over.
- No 0-RTT (the transport supports it; we do not), no connection migration, no Alt-Svc cache, no Happy Eyeballs in v1. The client is told to use h3, or not.

**The deduplication layer is ours, and it is a public type.** `quinn-proto`'s `Endpoint` owns the CID demultiplexer and creates a connection from the first packet of a new CID, before any handle exists. It cannot live in the transport layer, which knows nothing about HTTP, and it must not leak quinn's types into the API. So h3 has its own endpoint:

- `h3::Endpoint::new(udp: UdpId, config)` — one per UDP socket.
- `endpoint.feed(datagram, meta)` — hands a received datagram to the demux; it may produce a `New` for a connection the peer opened.
- `endpoint.accept() -> Option<h3::ConnId>` — the accept path for QUIC, and the only place a server-side h3 connection is born.
- `endpoint.conn(id)`, `endpoint.output_datagram()` — egress, which the application sends on the same `UdpId`.

h3 connections are therefore **not** `ConnId`s: one UDP socket carries many of them, and the driver keeps treating that socket as an opaque datagram pipe. This is the one place where the handle shape differs by protocol, and it is visible in the types rather than hidden behind one.

---

### 9.5 Headers

The typed model is an identity plus a span, not an enum of known fields:

- `HeaderName` is an interned identity. Registered names resolve to a `u16` id at compile time (`HeaderName::CONTENT_LENGTH`); unknown names are a validated byte range compared case-insensitively. Both forms live in one type, so an extension header like `traceparent` is first-class without becoming a variant.
- `HeaderValue` is a validated byte range (no CTLs; no whitespace the protocol forbids) with lazy typed accessors: `content_length() -> Option<u64>`, `connection_tokens()`, `transfer_coding()`. Parsing happens when asked for, never in the parser's hot path.
- `Fields` is an ordered, duplicate-preserving collection with a small inline representation: `push`, first match, all matches, iterate. Lookup compares the id first and falls back to a byte compare, so there is no `HashMap` and no `String` allocation.
- Wire fidelity is explicit: field order and duplicates survive, unknown names and values relay byte-exactly, and the proxy's hop-by-hop strip is a method over the typed list, not a string set.

This type sits on the hottest path in the library — every header of every request — and it is simultaneously what the six relay pairs depend on. That is exactly why it is an identity-plus-span: a closed enum cannot relay `x-forwarded-for`, and the `HashMap<String, String>` version is what the brief rejects.

---

## 10. TLS

rustls, sans-I/O, as a layer inside `Conn`. ALPN is chosen from the configured protocol set. Certificate configuration is explicit (no ambient roots for the server; the client takes a verifier or the bundled roots as a separate opt-in feature).

`close_notify` is sent on `finish` and detected on read; a truncated close is an error, not a clean EOF. The handshake never blocks the pump: handshake bytes are ordinary ciphertext in the `feed`/`output` path, which is the entire reason for choosing a sans-I/O TLS library.

Concretely, on TCP: `read_tls(&mut dyn Read)` and `write_tls(&mut dyn Write)` move bytes through rustls' internal buffers, which is exactly why §7 charges two copies inbound and one outbound. `send_close_notify` queues the notification and the next `write_tls` flushes it; `IoState::peer_has_closed()` reports the peer's. Half-close is the caller's to perform: queue, flush, shut down the write side, keep reading — rustls owns no socket, which is the property that lets it live inside `Conn` without a thread of its own.

TLS ships before h3 (M2), because h3 drives the same rustls through `rustls::quic`, and it is much cheaper to debug certificate and ALPN configuration on TCP.

---

## 11. Workers

The application owns the threads. libhttp ships the parts that make a pool safe:

- A bounded pump→worker queue of exchanges. Not unbounded, ever.
- A bounded worker→pump queue for responses and body requests, with a single wakeup primitive (`drv.wake()`), so a worker never touches a socket or a driver table.
- Parking with a deadline. Every blocking call on a worker takes a `Deadline`; there is no "block forever" variant. `try_` twins exist for callers that cannot afford to park.
- A per-connection cap on parked workers. A peer that opens many streams and reads none cannot convert the pool into a parking lot; the cap is enforced when the pump dispatches a stream, and the excess is reset rather than queued.
- Cancellation with one owner: the pump fires the deadline from the driver's timer heap and wakes the parked worker with `TimedOut`. The worker returns promptly; the stream is reset and its buffers returned.

A worker never sees a `ConnId`, a `Chunk` from the socket path it must manage, or a driver. It sees an exchange: a typed head, a body reader, a response writer. Body bytes reach it as chunks in a mailbox, so "blocking on the body" is blocking on an in-memory queue with a deadline — not on a socket. That is what makes the sync handler legitimate.

---

## 12. The convenience layer

`Server` and `Client` exist so a normal application does not have to write the pump loop, and they are written against exactly the public API above. They own the threads they spawn; they are behind the `threads` feature, so a build that wants no thread spawning from the library can exclude them entirely.

`Handler` is an ordinary function taking a mutable `Exchange`: typed head, streaming body, streaming response, trailers. No `Body` trait, no `Service`, no boxing per request.

If `Server` needs something the public API does not expose, the low-level API is wrong and gets fixed rather than given a back door. That rule is the price of the proxy being the acid test.

Name resolution is not in the core. `Driver::connect` takes a `SocketAddr`, because resolution blocks and there is no honest way to do it on the pump thread. The convenience layer resolves: `Client` and the proxy accept a host, resolve once per connection on a worker thread they own, and connect to the resulting address. v1 uses the first address returned and does not race endpoints — Happy Eyeballs is deferred, and saying so is better than half-supporting it.

The proxy ships as a library type plus an example binary with a static config; it is not a separate product.

---

## 13. The proxy

The proxy is a state machine on the pump thread, built only from public types. It never blocks and never needs a worker.

- h1↔h1, h1↔h2, h2↔h2, h1↔h3, h2↔h3, h3↔h3 — all of them, because the event vocabulary is shared; the only per-protocol code is connection setup.
- Bodies relay as chunk handles: no copy, one refcount bump per stream split, and a batched return if the two connections live on different shards.
- Hop-by-hop fields are stripped by a typed list, not a string set.
- Flow control is propagated by construction: the relay does not read from an engine whose downstream reservation is full, and does not grant upstream credit until a chunk is dropped. RST and GOAWAY propagate as events.
- CONNECT and `101` become `Tunnel` pairs; half-close is forwarded in both directions, and a half-closed tunnel keeps reading the other way until it too closes or times out.

The v1 proxy is single-driver-thread per instance. Multi-shard relaying is possible with the same primitives (batched cross-shard chunk return) but is not required for correctness, and is not in the gate.

---

## 14. The twelve open questions, answered

1. **I/O model across platforms.** Readiness-shaped public contract; level-triggered; completion-based I/O only underneath it. A completion API was rejected because it cannot be expressed on kqueue without a helper thread and an operation lifecycle the caller must manage, while readiness on top of completions is invisible. Windows is the case that settles it: it has no documented readiness API for writes at all, so readiness comes from AFD polling with `WSAPoll`+`ConnectEx` as the fallback, and a completion-based contract would have looked fine until this was discovered. §4.3, §6.7.
2. **Arming and backpressure.** No arming: interest is a per-connection flag, readiness repeats while it is true. Three backpressure signals, all of them just "stop calling": stop calling `recv`, stop calling `accept`, and let `pool.get` fail at its cap. `close` removes the socket and delivers one `Closed`; `finish` flushes `close_notify`/GOAWAY then half-closes. There are no operation ids and no completions, so there is no id lifetime to define. §6.1–6.8, §7.
3. **Buffers.** Pooled, refcounted chunks with spans; split by refcount, never by copy; batched cross-thread returns; write requires ownership; fixed headroom for length back-fill. `get` fails at the pool cap instead of allocating past it, and body chunks carry a credit token that returns stream window credit when the last view drops. The copy budget is a table, not a claim. §7.
4. **Workers.** Application-owned threads; library-provided bounded queues, parking with deadlines, per-connection parked-worker caps, pump-owned cancellation. No global blocking, no worker that touches a socket. §11.
5. **Fast-path scope.** v1 ships the portable A path on every platform. Phase 2 adds, each behind its own benchmark gate: io_uring write batching, `SEND_ZC`, splice and `sendfile`-with-`sf_hdtr` for file bodies, `TransmitFile` on Windows, batched UDP, and batched completion retrieval. Three facts are written into those ops rather than discovered later: `SEND_ZC` has two completions and the second means "buffer reusable", never "data delivered"; a notification can arrive after the socket is closed, so retained chunks are accounted per ring and close never waits on them; and multishot recv needs provided buffers, which a per-call buffer API cannot supply — so reads stay single-shot in v1 and that is the stated price of the chosen boundary.
6. **QUIC backend.** `quinn-proto` 0.11 — the only pure-Rust sans-I/O QUIC transport in the field; quiche needs BoringSSL and cmake, s2n-quic mandates tokio, neqo needs NSS, gm-quic is async-bound. Its MSRV is 1.85, the same as ours. The boundary copies in both directions (§9.4): once per inbound datagram into `BytesMut`, once per outbound transmit batch. Per-datagram ECN is not available on its datagram surface, and the design says so instead of pretending. No 0-RTT, no migration, no discovery in v1. HTTP/3 framing and QPACK are ours.
7. **TLS and milestone order.** rustls inside `Conn`; TLS at M2, before h2's gate, and h3 reuses the same configuration through the QUIC backend. §10, §15.
8. **File bodies.** Not in the first milestone. The application streams chunks it reads on its own thread; sendfile/splice is a phase-2 op with the same gate as the rest of B. No file I/O on the pump thread, ever. §15.
9. **Half-close.** `shutdown_write` after `close_notify`; tunnels carry it; every backend implements it or the backend is not finished. §6.5.
10. **Socket ownership.** Driver-owned authoritative tables, generational ids, no adoption, no fd in the API, abort and graceful close as distinct operations. §6.4.
11. **Performance gate.** Attached to M1 and M2, which can actually serve the endpoints: A-vs-hyper on the same host, recorded rustc/kernel/hyper, six metrics on every platform. A miss keeps the milestone open — fix it or waive it in writing. §15.
12. **Crate layout.** One crate, features `tls`, `h3`, `proxy`, `threads`, `roots`; MSRV 1.85; dependency list with a reason per entry, `ring` pinned as the crypto provider and quinn-proto's defaults disabled. §16.

---

## 15. Milestones and gates

**M1 — transport and HTTP/1.1.**
Driver on all three platforms with the A path, chunks and pools, the h1 engine, `Conn`, h1↔h1 proxy, fuzz targets for the parser and the engine. No TLS, no h2, no h3.
*Gate 1:* h1 throughput, p50/p99, syscalls, allocations and copies against hyper on the same host; 10k idle connections per reactor thread with measured memory.

**M2 — TLS and HTTP/2.**
rustls in `Conn`; h2 frames, HPACK, flow control, the credit policy; h2 client and server; proxy h1↔h2 and h2↔h2.
*Gate 2:* TLS+h2 against hyper on the same host, plus the copy count for the TLS path measured against the table in §7.

**M3 — HTTP/3.**
quinn-proto behind the datagram edge, our framing and QPACK, h3 client and server, the remaining relay pairs, interop against an independent implementation run either as a dev-dependency or as a separate process, so a second QUIC stack never enters the product graph.
*Gate 3:* correctness via interop, plus published h3 numbers against the interop peer. There is no hyper h3 to compare with, so the number is published as-is rather than as a win.

**M4 — accelerators.**
Each fast path lands separately, behind its own gate, on the platform where it applies: io_uring writes, `SEND_ZC`, splice, `sendfile`, `TransmitFile`, batched UDP, batched completion retrieval. A path that does not beat A on the published harness stays off by default.

The ordering rule that matters: no milestone is gated on a fast path that lands later. M1 and M2 are measured on the A path.

### Gate policy

A milestone is an exit condition, not a date. Its exit condition has two clauses: the functional suite passes, and the gate measures. Correctness blocks unconditionally — there is no waiver for a parser that accepts a CL/TE conflict. Performance blocks by default and can be waived explicitly.

Each gate stands up the milestone's endpoint and a hyper equivalent on the same host with the same rustc and kernel, runs two endpoints (a trivial GET, and a 1 MiB body through the proxy), and records throughput, p50, p99, syscalls, allocations and copies on every supported platform.

Pass condition, on every platform:

- throughput ≥ 95% of hyper;
- p99 ≤ 110% of hyper;
- copies and allocations no worse than hyper;
- all six numbers published, with the harness.

A miss keeps the milestone open. Diagnose which of the six is off and fix it, or waive it in writing, with the measured gap and a follow-up item. What is forbidden is closing the milestone quietly with the number unexamined. That is the whole policy: the gate exists to fail early, when a structural deficit costs a tuning pass rather than a rewrite of four protocol layers.

The margin is deliberately not "beat hyper". M1's job is to prove the shape is sound on the portable path; winning is what M4 is for.

### The memory budget

"Bounded" needs a number, or the 10k-idle gate has no pass condition. These are design targets, checked and reported at Gate 1; the measured value goes into the gate record whether or not it meets the target.

| State | Target owned by the library | Dominant term |
|---|---|---|
| Idle h1 keep-alive connection | ≤ 1 KiB | parser state; no pooled chunk is retained between requests |
| Idle h2 keep-alive connection | ≤ 8 KiB | HPACK dynamic table (4 KiB by default, configurable) |
| Idle h3 connection | ≤ 32 KiB | quinn-proto loss-recovery and crypto state |
| TLS, on top | ≤ 20 KiB | rustls' deframer holds up to one 16 KiB record |
| Chunk pool, per shard | ≤ 64 MiB | byte cap; `get` fails at the cap (§7) |

So 10k idle connections cost roughly 10 MiB (h1), 80 MiB (h2) or 320 MiB (h3) before TLS, and the gate reports the real figure. If the h3 number is unacceptable, the knob is a connection limit, not a smaller QUIC state — shrinking the transport's state is not something this design can honestly offer.

---

## 16. Crate layout, features, dependencies

One crate, `libhttp`. Edition 2024, MSRV 1.85.

| Feature | Adds | Why optional |
|---|---|---|
| `tls` | rustls, certificate config, `close_notify` | Some deployments terminate TLS elsewhere |
| `h3` | quinn-proto, QPACK, the datagram edge | Drags the largest dependency; h1/h2 users should not pay for it |
| `proxy` | the relay state machine | Leaf functionality; builds only on public API |
| `threads` | `Server`, `Client`, `Workers` spawning | Keeps "the core spawns no threads" true by construction |

| Dependency | Reason |
|---|---|
| `rustls` | Sans-I/O TLS; the only TLS stack considered that does not bring a runtime |
| `ring` | rustls' crypto provider: needs a C compiler, but not CMake or NASM, which aws-lc-rs does |
| `webpki-roots` | Client trust store, opt-in; never ambient OS roots |
| `quinn-proto` 0.11 | Sans-I/O QUIC transport, which the brief delegates; pure Rust, no runtime, MSRV 1.85 |
| `libc` | Unix syscalls (epoll, kqueue, sockets) |
| `windows-sys` | IOCP, AFD ioctls, socket options |
| `io-uring` | Phase 2 Linux accelerator only |

`rustls` and `quinn-proto` are configured deliberately, because their defaults contradict this design: rustls defaults to aws-lc-rs, which wants CMake and NASM on Windows, and quinn-proto's default features pull both crypto providers plus `rustls-platform-verifier`, which reads ambient OS roots. We pin `ring`, disable quinn-proto's default features, and make the root store an explicit feature.

The "no C toolchain" claim in the brief applies to the QUIC *transport*, and quinn-proto satisfies it by being pure Rust. It does not apply to the crypto provider: `ring` compiles C and assembly on every platform, through the same compiler every supported CI image already has.

Dev-only, never in the product: hyper (the benchmark baseline), an independent h2/h3 implementation (interop), a fuzzer harness.

QUIC transport alternatives, and why they lost: quiche needs BoringSSL and a C toolchain; s2n-quic mandates tokio and defaults to a C TLS stack; neqo needs NSS; gm-quic is async-bound. The choice is not close.

Not used, deliberately: `bytes`, `httparse`, `mio`, `tokio`, `hyper`, `h2`, `h3`. No buffer library: the chunk design is the point.

---

## 17. What is left to the builder

Internal choices, not public contracts:

- Pool size classes, ring depths, reservation sizes, and the blocked-stream default — constants, tuned against the harness.
- Huffman table representation (shared by HPACK and QPACK).
- The benchmark harness topology and the host; the gate compares on one host and records it.
- Whether the `Conn` dispatch on engine is a vtable or a generic; both are per-packet, not per-byte, so either is acceptable.

### Platform risk

The Windows backend depends on an internal NT interface, and that is the one platform decision here that is not purely a performance decision. It is the same interface mio and wepoll have shipped on for years, and the `WSAPoll`+`ConnectEx` fallback keeps correctness if it changes. The fallback costs O(n) per poll and cannot be called a fast path. If it ever has to become the only path on Windows, the design still holds; the numbers do not.

Nothing else in the design depends on an undocumented interface: epoll, kqueue, `sendfile`, `TransmitFile`, `SEND_ZC` and provided buffers are all documented.

Deliberately *not* left to the builder: readiness and interest, close and half-close, chunk ownership and the copy budget, cancellation and deadlines, and the worker boundary. Those are above, and they are the design.

---

## 18. Verification

- Fuzz targets: h1 parser, h2 frame/HPACK decoder, h3/QPACK decoder, all reachable from a byte slice with no I/O.
- Conformance: h1 smuggling corpus; h2 and h3 interop against an independent implementation, both as client and as server.
- Portability: CI runs the functional suite on Linux, macOS and Windows on every change; the benchmark numbers are per platform and never extrapolated.
- Performance: the harness is published with the numbers, including syscalls, allocations and copies, so a bad result is reproducible rather than argued about.

---

## Appendix A — Platform facts this design relies on

Checked against primary sources on 2026-10-26. These are the facts that would change the design if they turned out to be wrong.

| Fact | Source |
|---|---|
| kqueue is level-triggered by default; `EV_CLEAR` is the edge mode | [FreeBSD kqueue(2)](https://man.freebsd.org/cgi/man.cgi?query=kqueue&sektion=2) |
| `sendfile` exists on macOS (Darwin 9.0+) and FreeBSD and takes `sf_hdtr`, so headers and trailers need no extra `writev` | [Apple sendfile(2)](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/sendfile.2.html), [FreeBSD sendfile(2)](https://man.freebsd.org/cgi/man.cgi?query=sendfile&sektion=2) |
| `WSAPoll` passes the array on every call (O(n), no persistent interest set) and never reports a failed connect | [Microsoft WSAPoll](https://learn.microsoft.com/en-us/windows/win32/api/winsock2/nf-winsock2-wsapoll), [curl, 2012](https://daniel.haxx.se/blog/2012/10/10/wsapoll-is-broken/) |
| A zero-byte overlapped `WSASend` completes immediately even with `SO_SNDBUF` full, so write readiness cannot be probed | empirical report, treated as a design constraint ([source](https://stackoverflow.com/posts/42019668/timeline)) |
| mio 1.x and wepoll obtain readiness from `IOCTL_AFD_POLL` on `\Device\Afd`, completions through IOCP | [mio afd.rs](https://docs.rs/crate/mio/1.2.4/source/src/sys/windows/afd.rs) |
| `GetQueuedCompletionStatusEx` returns a batch of completions in one call | [Microsoft](https://learn.microsoft.com/en-us/windows/win32/fileio/getqueuedcompletionstatusex-func) |
| `SEND_ZC` produces two CQEs; the `F_NOTIF` one means "buffer reusable"; errored requests still notify | [io_uring_enter(2)](https://manpages.debian.org/unstable/liburing-dev/io_uring_enter.2.en.html), [io_uring_prep_send_zc(3)](https://manpages.debian.org/testing/liburing-dev/io_uring_prep_send_zc.3.en.html) |
| Multishot recv requires provided buffers (a per-call buffer fails `EINVAL`); `-ENOBUFS` means no buffer in the group | [liburing issue 1331](https://github.com/axboe/liburing/issues/1331), [io_uring_prep_recv(3)](https://manpages.debian.org/testing/liburing-dev/io_uring_prep_recv.3.en.html) |
| Emulated readiness can be spurious; callers must handle `EAGAIN` | [libuv poll](https://docs.libuv.org/en/v1.x/poll.html) |

---

## Appendix B — QUIC and TLS facts this design relies on

Checked against primary sources on 2026-10-26.

| Fact | Source |
|---|---|
| `quinn-proto` 0.11.19 is sans-I/O — no sockets, no runtime — and its MSRV is 1.85 | [crates.io](https://crates.io/api/v1/crates/quinn-proto), [docs.rs](https://docs.rs/quinn-proto/0.11.19/quinn_proto/) |
| `Endpoint::handle` takes inbound packets as `BytesMut`; `poll_transmit(now, max_datagrams, &mut Vec<u8>)` writes egress into caller-owned storage and returns sizes, not buffers | [docs.rs](https://docs.rs/quinn-proto/0.11.19/quinn_proto/struct.Connection.html) |
| RFC 9221 datagrams use `Datagrams::send(Bytes)` / `recv() -> Option<Bytes>`; that surface carries no ECN | [docs.rs](https://docs.rs/quinn-proto/0.11.19/quinn_proto/struct.Datagrams.html) |
| quiche requires BoringSSL and cmake; s2n-quic mandates tokio and defaults to a C TLS stack; neqo uses NSS; gm-quic is async-bound | [quiche README](https://raw.githubusercontent.com/cloudflare/quiche/master/README.md), [s2n-quic](https://crates.io/api/v1/crates/s2n-quic/1.91.0), [neqo](https://raw.githubusercontent.com/mozilla/neqo/main/README.md), [gm-quic](https://crates.io/api/v1/crates/gm-quic/0.4.0/dependencies) |
| rustls 0.23 TCP surface is `read_tls`/`write_tls`/`process_new_packets`, copying through internal buffers; QUIC lives in `rustls::quic` | [rustls::quic](https://docs.rs/rustls/0.23.46/rustls/quic/index.html), [ConnectionCommon](https://docs.rs/rustls/0.23.46/rustls/struct.ConnectionCommon.html) |
| `send_close_notify` queues, `write_tls` flushes, `IoState::peer_has_closed()` detects; rustls owns no socket, so half-close belongs to the caller | [IoState](https://docs.rs/rustls/0.23.46/rustls/struct.IoState.html) |
| The `h3` crate keeps QPACK private outside an unstable feature, and the standalone `qpack` extraction is WIP — neither is a dependency, both are references | [h3 source](https://raw.githubusercontent.com/hyperium/h3/master/h3/src/lib.rs), [qpack 0.1.0](https://docs.rs/crate/qpack/0.1.0) |
| The QPACK static table has 99 entries; the dynamic table is FIFO and an entry costs name + value + 32 bytes | [RFC 9204](https://www.rfc-editor.org/rfc/rfc9204.html) |
