# libhttp transport — Design

**Status:** subsystem design, rewritten after verification. It owns the seam contracts the later documents consume; every type those documents name is defined here.
**Parent:** `2026-10-26-libhttp-design.md` — intent, goals, architecture, milestones, gates.
**Siblings that consume this:** `conn` (feed/poll/output/finish/tick/arm), `tls`, `h1`, `h2`, `h3`, `dispatch`, `proxy`.

Everything here is normative. §9 is the only deferral, and it fixes the public shape of what it defers.

---

## 1. Scope

**Owns:** sockets, readiness, interest, timers, ids, datagrams, the chunk pool, credit sinks, close and half-close.

**Does not own:** HTTP, TLS, worker dispatch, graceful drain (the `conn` document owns `finish`/`Drained`), or the pump loop. No HTTP type appears here, and no code here spawns a thread.

---

## 2. Types

```rust
#[non_exhaustive]
pub enum Error {
    StaleId,
    Exhausted,        // the pool cap was reached
    NotActivated,     // recv on a connection whose reads are not open
    HalfClosed,       // send after shutdown_write
    Io(std::io::Error),
    Connect(ConnectError),
}

#[non_exhaustive]
pub enum ConnectError { Refused, Unreachable, TimedOut, Other(i32) }

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Interest(u8);              // const READ, WRITE; empty mask is legal
impl Interest { pub const READ: Interest; pub const WRITE: Interest;
                pub fn contains(self, other: Interest) -> bool;
                pub fn is_empty(self) -> bool; }
impl std::ops::BitOr for Interest { /* ... */ }

pub enum Read { Data(usize), WouldBlock, Eof }
pub enum Sent { All, Partial(usize), WouldBlock }

#[non_exhaustive]
pub enum TimerKind {
    Idle, ReadProgress, WriteProgress, Drain, HalfClose,
    Worker { exchange: u64 },         // per exchange, not per connection
}

#[non_exhaustive]
pub enum CloseReason {
    Local,                 // close() by the application
    Peer,                  // clean FIN
    PeerReset,             // RST
    Io(i32),               // another socket error
}

pub struct Config { pub pool: PoolConfig, pub batch_budget: usize }
impl Default for Config;               // 64 MiB pool, 512

pub struct PoolConfig { pub cap_bytes: usize, pub classes: &'static [usize] }
impl Default for PoolConfig;           // 64 MiB, classes &[4096, 17408, 66560] usable

pub struct Datagram<'a>    { pub bytes: &'a mut [u8], pub len: usize,
                             pub peer: std::net::SocketAddr, pub ecn: Option<Ecn> }
pub struct OutDatagram<'a> { pub bytes: &'a [u8],
                             pub peer: std::net::SocketAddr, pub ecn: Option<Ecn> }
pub struct Ecn(pub u8);

pub enum BackendPath { Epoll, Kqueue, Afd, WsaPoll }
```

`Datagram.len` and `.ecn` are **outputs** and are ignored on entry; `peer` is ignored and overwritten. `OutDatagram.ecn` is ignored where the platform does not support ECN (§4.7, Appendix).

---

## 3. Driver surface

```rust
impl Driver {
    pub fn new(config: Config) -> Result<Driver, Error>;
    pub fn wake_handle(&self) -> WakeHandle;                 // Clone + Send + Sync

    pub fn listen(&mut self, addr: SocketAddr) -> Result<ListenerId, Error>;
    pub fn local_addr(&self, l: ListenerId) -> Result<SocketAddr, Error>;
    pub fn unlisten(&mut self, l: ListenerId);
    pub fn connect(&mut self, addr: SocketAddr, timeout: Duration) -> Result<ConnId, Error>;
    pub fn accept(&mut self, l: ListenerId) -> Result<Option<(ConnId, SocketAddr)>, Error>;
    pub fn activate(&mut self, c: ConnId) -> Result<(), Error>;
    pub fn interest(&mut self, c: ConnId, i: Interest) -> Result<(), Error>;

    pub fn recv(&mut self, c: ConnId, buf: &mut ChunkMut) -> Result<Read, Error>;
    pub fn send(&mut self, c: ConnId, chunks: &[Chunk]) -> Result<Sent, Error>;
    pub fn close(&mut self, c: ConnId);
    pub fn shutdown_write(&mut self, c: ConnId) -> Result<(), Error>;

    pub fn bind_udp(&mut self, addr: SocketAddr) -> Result<UdpId, Error>;
    pub fn udp_local_addr(&self, u: UdpId) -> Result<SocketAddr, Error>;
    pub fn close_udp(&mut self, u: UdpId);
    pub fn recv_datagrams(&mut self, u: UdpId, out: &mut [Datagram]) -> Result<Read, Error>;
    pub fn send_datagrams(&mut self, u: UdpId, out: &[OutDatagram]) -> Result<usize, Error>;

    pub fn deadline(&mut self, c: ConnId, kind: TimerKind, after: Duration);
    pub fn cancel_deadline(&mut self, c: ConnId, kind: TimerKind);

    pub fn wait(&mut self) -> Result<(), Error>;
    pub fn next_io(&mut self) -> Option<Io>;
    pub fn mark_active(&mut self, c: ConnId);
    pub fn active(&self) -> impl Iterator<Item = ConnId> + '_;
    pub fn drain_wakes(&mut self);                            // fold WakeHandle marks into the active set
    pub fn wake(&mut self) -> Result<(), Error>;

    pub fn pause_reads(&mut self, c: ConnId);
    pub fn paused(&self) -> usize;
    pub fn pool(&self) -> &ChunkPool;
    pub fn pool_handle(&self) -> PoolHandle;                  // Clone + Send + Sync, for workers
    pub fn sink(&self, c: ConnId) -> Arc<CreditSink>;
    pub fn backend_path(&self) -> BackendPath;                // Afd | WsaPoll | Epoll | Kqueue
}
```

`Io` is the only delivery path for readiness and lifecycle:

```rust
pub enum Io {
    Acceptable(ListenerId),
    Connected(ConnId),
    ConnectFailed(ConnId, ConnectError),
    Readable(ConnId),
    Writable(ConnId),
    HalfClose(ConnId),
    Timer(ConnId, TimerKind),
    Closed(ConnId, CloseReason),
    Datagram(UdpId),
}
impl Clone for Io; impl std::fmt::Debug for Io;
```

Every method that takes an id returns `Error::StaleId` for a dead one except `close`, `unlisten`, `close_udp`, `mark_active`, `deadline` and `cancel_deadline`, which are idempotent no-ops. That split is deliberate: the idempotent ones are cleanup paths, and a cleanup path that returns an error is a cleanup path callers stop writing.

---

## 4. Contracts

### 4.1 Readiness

Level-triggered on every platform. `wait` reports a socket while it has readiness to report; the application never re-arms a socket.

Readiness is a hint: `recv` after `Readable` may return `WouldBlock`, which is an ordinary result. `recv` and `send` never block and never park the calling thread.

`wait` returns when any of these holds:

- `next_io` would yield at least one event;
- a timer is due;
- `batch_budget` events are ready to be yielded;
- a wake from a `WakeHandle` arrived;
- a paused connection's pool crossed the resume watermark (§4.2).

The budget bounds events, not time. One event can consume `READ_BUDGET` (256 KiB, owed by the caller: it is `READ_SIZE` × 16 with `READ_SIZE` = 16 KiB), so deadline lateness is `budget × per-event work` and is published as a measured number, not asserted. The budget exists so a burst cannot starve timers.

### 4.2 Interest, activation, and driver-side exceptions

`interest(c, mask)` **replaces** the mask. The application owns it. There are exactly three exceptions, and each is stated because the driver knows something the application does not:

1. **Activation.** `accept` and `connect` return connections with an empty mask. Nothing is reported until `activate(c)`. `recv` before activation is `Error::NotActivated`. This closes the accept→insert window: an id cannot be reported before the application has recorded it.
2. **Connect.** `connect` sets WRITE internally so it can observe completion, and clears it when `Connected` or `ConnectFailed` is delivered. The application's mask is preserved underneath and restored at delivery.
3. **The pool pause.** See below.

**The pause.** `pause_reads(c)` stores the application's current mask, clears READ, and parks the id. `wait` restores the stored mask, once, when `pool.free_bytes() >= pool.cap_bytes() / 2`. While paused, `next_io` filters `Readable` for that id — including events already queued in this batch — so the application never sees a read event it cannot act on. `pause_reads` on an already-paused id is a no-op; on a connecting id it is refused (no-op) because connect needs its WRITE.

The pause exists because a full pool with READ armed is a busy loop on a level-triggered backend. It applies to connections only: UDP backpressure belongs to the QUIC layer, and a listener has its own rule (§4.3).

### 4.3 Listener backpressure and failures

`accept` returns `Ok(None)` when nothing is pending (`EAGAIN`), so the application can drain a listener without blocking. A full backlog does not need a pause API: the application calls `accept` until `None`, and `Acceptable` repeats while connections are pending — the same level-triggered rule as reads, with no buffer to exhaust.

`ECONNABORTED` from `accept` is retried internally and never surfaced. `EMFILE`/`ENFILE` is `Error::Io`; the connection is not accepted and the listener keeps reporting `Acceptable`, so the application can back off and retry. `unlisten` closes the listener; queued `Acceptable` events for it are filtered.

**Connect is bounded.** `connect` arms its own deadline for `timeout`; when it elapses the driver closes the socket and delivers `ConnectFailed(c, TimedOut)`. There is no unbounded connect, and a connecting id reports `NotActivated` for `recv` and `Error::HalfClosed` for `send` until it completes.

### 4.4 EOF latch

The first `Read::Eof` on a connection sets a read-shut latch. The driver reports `HalfClose` once and stops reporting `Readable` for that id, because a FIN'd socket is readable forever. The latch is cleared only by `activate`, which is the application saying it wants reads again — and since a FIN'd socket has no more data, calling `activate` after `HalfClose` and without a new write is pointless; the contract permits it and documents that it re-reports `Eof` once. The spin cannot recur because the latch is only cleared on an explicit call.

`HalfClose` is not `Closed`: a peer that half-closes leaves the connection writable.

### 4.5 Send, partial writes, and half-close

`send` takes a **chain** and returns `Sent::{All, Partial(n), WouldBlock}` where `n` counts bytes across the chain in order.

- `All` — the whole chain is in the kernel.
- `Partial(n)` — `n` bytes written. The caller drops the first `n` bytes of the chain and retries the rest when `Writable` arrives.
- `WouldBlock` — nothing written, not an error.

The driver holds no output queue; a short write is the application's to resume. Outside §4.2's three cases the driver never touches the mask: in particular a `Partial` or a `WouldBlock` does not turn WRITE on by itself — the application does, because only it knows whether it has more to send.

`shutdown_write(c)` is `shutdown(SHUT_WR)` on Unix and `shutdown(SD_SEND)` on Windows. It is legal only when the caller has no partial chain outstanding, and the driver cannot detect a violation; that is why it is a precondition, not a check. Calling it twice is a no-op. `send` after it returns `Error::HalfClosed`.

**SIGPIPE.** `MSG_NOSIGNAL` on Linux, `SO_NOSIGPIPE` on macOS/BSD, nothing on Windows (which has no SIGPIPE). The application never sees SIGPIPE from this library.

**Reset mid-read.** A peer RST during `recv` surfaces as `Error::Io(ECONNRESET)` and schedules `Closed(ConnId, PeerReset)`. Bytes already returned by earlier `recv` calls remain valid; the reset does not retroactively invalidate a chunk.

### 4.6 Close

`close(c)` — abort. Remove the socket, purge every timer slot, mark the credit sink closed (not freed), deliver one `Closed(ConnId, Local)`. It does not free storage the application holds; force-reclaiming would underflow the refcount when the application's copy drops.

**Who produces each `CloseReason`.** `Local` — `close`. `Peer` — EOF on a connection whose write side is already shut down, which is the graceful ending and the only one that is. `PeerReset` — RST. `Io(code)` — any other socket error while reading or writing. A clean EOF with the write side still open is `HalfClose`, not `Peer`, because the connection is still writable; `Peer` arises when the application has already called `shutdown_write` and the peer then finishes.

`Closed` is delivered exactly once and is the last event an id ever produces. Any event queued for it in the same batch — including a `Writable` or a `Timer` that preceded the close — is filtered before delivery.

There is no `finish` or `Drained` here: graceful drain needs to know whether the connection layer has more output to produce, which this layer cannot see. `shutdown_write` plus `TimerKind::Drain` are the transport's whole contribution; `conn` owns the rest.

### 4.7 Datagrams

`recv_datagrams(u, out)` returns `Read::{Data(n), WouldBlock, Eof}`: `Data(n)` is the number of datagrams written into `out`. It returns `WouldBlock` rather than `0` so an empty `out` slice and an empty socket are distinguishable (an empty `out` is a caller bug and yields `WouldBlock`).

A datagram larger than one `Datagram::bytes` is truncated by the kernel; `len` is what arrived, and the caller detects truncation by `len == bytes.len()`. Truncation is not an error and not a retry.

`send_datagrams(u, out)` returns how many **prefix** entries were sent. A partial batch is legal: the caller retries from the returned index when `Datagram(u)` is reported writable again. `EMSGSIZE` for one entry fails the whole call with `Error::Io`. UDP send-buffer exhaustion is never a spin: a short count means "wait for `Datagram(u)`", and the driver reports `Datagram(u)` on write readiness only while a short send is outstanding, mirroring §4.2's write interest.

`ecn` is `Some` only where the platform provides it (Appendix); `Some` on an unsupported platform is ignored, never an error.

### 4.8 Timers

One heap, one clock, one owner: the pump thread. Slots are keyed by `(ConnId, TimerKind)`, so re-arming `Idle` does not disturb `WriteProgress`.

- `deadline(c, kind, after)` sets the slot to `now + after`; `cancel_deadline(c, kind)` clears it.
- `wait` derives its timeout from the earliest slot.
- Each slot carries a generation bumped on set and on cancel; stale events are dropped at delivery.
- `closed` purges every slot for the connection.

`TimerKind` semantics, all of them just "report":

| Kind | Set by | Meaning when it fires |
|---|---|---|
| `Idle` | conn layer | the connection has been idle too long |
| `ReadProgress` | conn layer | no bytes read for too long while a body was expected |
| `WriteProgress` | conn layer | no bytes written for too long while a body was pending |
| `Drain` | conn layer | graceful shutdown has not completed in time |
| `HalfClose` | proxy | a tunnel half-closed and the other side has not finished |
| `Worker { exchange }` | dispatch | the parked handler's budget is spent |

The transport never closes on a timer. It reports `Io::Timer(conn, kind)`; the connection layer or the proxy decides what that means, and calls `close` if it means abort. `TimerKind::Worker` carries the exchange id so a per-exchange deadline identifies its exchange — one connection can have many.

### 4.9 The active set and wakes

`mark_active(c)` records that the application has work for `c` this iteration — after feeding bytes, after producing output, after a deadline, after a wake. `active()` yields those ids. The set is cleared at the start of each `wait`; its storage is a `Vec<ConnId>` reused across iterations, one push per mark and no allocation after warm-up. Duplicates are permitted and harmless: polling a connection twice in one iteration is idempotent by contract.

`WakeHandle::wake(c)` is callable from any thread. It pushes `c` onto a mutex-protected inbox; `wait` drains it (`drain_wakes`) and merges it into the active set before scanning. `WakeHandle::wake_pump()` (no id) only interrupts `wait` — used by the pool's cross-thread return path, so a pump blocked in `wait` cannot sleep past a return that would cross the resume watermark.

`mark_active` and `active` are the only reason the loop does not scan every connection; a connection never marked is never polled.

---

## 5. Chunks, pool, credit

### 5.1 Chunk

```rust
pub struct ChunkMut { /* ... */ }
impl ChunkMut {
    pub fn bytes_mut(&mut self) -> &mut [u8];
    pub fn len(&self) -> usize;
    pub fn truncate(&mut self, len: usize);
    pub fn freeze(self) -> Chunk;
}

pub struct Chunk { /* ... */ }
impl Chunk {
    pub fn len(&self) -> usize;
    pub fn slice(&self, r: std::ops::Range<usize>) -> Result<Chunk, SliceError>;
    pub fn as_ptr(&self) -> *const u8;
}
impl std::ops::Deref for Chunk { type Target = [u8]; }
impl Clone for Chunk;                    // one atomic increment
pub enum SliceError { OutOfRange }

pub struct ChunkPool { /* ... */ }
impl ChunkPool {
    pub fn new(config: PoolConfig) -> ChunkPool;
    pub fn get(&self, size: usize) -> Result<ChunkMut, Error>;
    pub fn free_bytes(&self) -> usize;
    pub fn cap_bytes(&self) -> usize;
}
pub struct PoolHandle { /* Arc<ChunkPool> + Send + Sync */ }
impl PoolHandle { pub fn get(&self, size: usize) -> Result<ChunkMut, Error>;
                  pub fn free_bytes(&self) -> usize; }
```

- `ChunkMut` is `Send`. It is uniquely owned until `freeze`; writing requires it, and the type system enforces that. `len` on a fresh `ChunkMut` is its class's usable size; `recv` writes at most that and the caller calls `truncate(n)`.
- `Chunk: Clone + Send`, **not `Sync`**: mutation only exists through `ChunkMut`, and sharing an `&Chunk` across threads would let two `&[u8]` views disagree with the headroom rule. That is the reason the brief asks for.
- The refcount is atomic on every path, because a clone can cross a thread while another stays. One uncontended atomic increment per clone, one decrement per drop; a split is a clone plus a drop.
- Dropping the last reference returns storage to the owning pool; cross-shard returns batch into that pool's inbox and call `wake_pump()` once per batch.

**The outbound path is the same pool.** Any layer that needs to write allocates a `ChunkMut`, writes into `bytes_mut`, truncates, and freezes. There is no second allocator and no static chunk type.

**Framing never writes into shared storage.** A read slice may begin at offset 0 of its storage with another stream's payload immediately before it, so headroom on the *allocation* is not headroom on the *slice*. Framing uses a chain: the header is its own chunk, the payload is the next, and `send` gathers them with one `writev`. Headroom is only for chunks a layer allocates for itself, where a length is back-filled before the payload exists.

### 5.2 Pool

Per shard, classed by usable size, bounded by total bytes. A `get` first takes from a thread-local free list (the pump's fast path), then from the shared mutex-protected list, then allocates if under the cap; at the cap it returns `Error::Exhausted`. `free_bytes` and `cap_bytes` are exposed because the pause rule needs both.

Classes default to 4 KiB, 17 KiB and 65 KiB usable, with 16 bytes of headroom and 2 of tailroom added on top. The exact numbers are `PoolConfig`; the cap is what bounds memory §7.

### 5.3 Credit sinks

```rust
pub struct CreditSink { /* Arc-shared; generation + closed flag + inbox */ }
impl CreditSink {
    pub fn credit(&self, stream: u64, bytes: u32);   // sendable from any thread
    pub fn drain(&self) -> CreditBatch;               // pump thread
    pub fn is_closed(&self) -> bool;
}

pub struct CreditBatch { /* (stream, bytes) pairs; no allocation after warm-up */ }
impl CreditBatch {
    pub fn iter(&self) -> impl Iterator<Item = (u64, u32)> + '_;
    pub fn is_empty(&self) -> bool;
    pub fn clear(&mut self);
}
```

`Driver::sink(c)` returns the connection's sink. The sink outlives the connection: `close` sets its closed flag and drops its contents; a handle dropped afterwards credits a dead sink instead of a recycled id. `drain` returns the aggregated `(stream, bytes)` pairs and clears the inbox; `conn.tick` calls it and returns the credit to the engine, which is why `tick` can require output.

`credit` is lock-free in the uncontended case (an atomic per pair appended to a preallocated slab); the design does not pretend otherwise when it is contended.

---

## 6. Backends

One internal trait: `register`, `reregister`, `deregister`, `wait`, `next`, `wake`.

- **Linux:** epoll, level-triggered (never `EPOLLET`); `epoll_ctl` only when the mask changes. `wake` is an `eventfd`.
- **macOS/BSD:** kqueue, `EVFILT_READ`/`EVFILT_WRITE`, level-triggered (no `EV_CLEAR`). `wake` is `EVFILT_USER`.
- **Windows:** `IOCTL_AFD_POLL` on `\Device\Afd`, completions through the same IOCP with `GetQueuedCompletionStatusEx`. AFD polls are one-shot, so the backend re-issues one after each completion. `wake` is `PostQueuedCompletionStatus`.

A duplicated epoll fd cannot wake `epoll_wait` — that is why `wake` is per-backend and not a dup. `WakeHandle` holds one per platform, `Clone + Send + Sync`, and exposes `wake(ConnId)` and `wake_pump()`.

### 6.1 Windows fallback and self-test

The documented fallback is `WSAPoll` for read/write readiness plus `ConnectEx` for connect. `WSAPoll` could not report a failed connect before Windows 10 2004; on current Windows it can, but the fallback keeps `ConnectEx` because support for older builds is cheap and the failure mode of getting it wrong is a hung connect.

Before use, a **loopback TCP pair** (Windows has no `socketpair`) exercises: read readiness, write readiness, a peer abort producing `PeerReset`, and re-reporting after a completion whose event was deliberately not drained. Any mismatch selects `WsaPoll` and records it in `backend_path()`.

### 6.2 No completion probes

A completion op for I/O cannot report writability: a zero-byte `WSASend` completes immediately even with `SO_SNDBUF` full, and an `io_uring` `SEND` does not tell you whether the next one would block. Poll-shaped completions can report readiness (AFD is exactly that; `IORING_OP_POLL_ADD` is another), but that is a readiness mechanism with an extra step, not a completion API for I/O. So the Windows backend uses AFD — a poll — and `send` is a syscall everywhere.

---

## 7. Budget

| Item | Target | Note |
|---|---|---|
| Pool per shard | 64 MiB cap | 512 MiB at 8 shards, the largest single line item |
| Idle connection, no partial read | ≤ 1 KiB | connection state only; no chunk retained |
| Partial request head held | one 17 KiB class chunk | a 16 KiB read lands in the second class |
| Per-event read budget | 256 KiB | bounds one socket's share of a `wait` |
| Batch budget | 512 events | bounds a `wait`'s event count, not its wall time |

Copies at this layer: kernel → chunk inbound (1), chunk → kernel outbound (1). Atomics: one allocation and one free per chunk; one increment per clone and one decrement per drop.

---

## 8. What a builder decides

The exact class sizes beyond the stated usable numbers, the free-list growth policy, the backend event buffer depth, whether `ChunkPool` uses a per-class mutex or a single lock, and the syscall wrapper layout.

Not theirs: readiness, the three interest exceptions, the EOF latch, `Sent`, close versus half-close, timer ownership and kinds, id staleness, the pause rule, the pool cap, the wake model.

---

## 9. Deferred

Only one thing, and its public shape is fixed here so no later document has to change an API: **`send_zc`** (phase 2, M4).

```rust
pub fn send_zc(&mut self, c: ConnId, chunks: &[Chunk]) -> Result<Sent, Error>;
```

Semantics: `Sent::All` means "submitted and retained", never "written"; `Partial` and `WouldBlock` mean nothing was retained. Retained chunks are held by the ring, not the pool, and the pool does not reclaim them on refcount zero during that window. Because errored `SEND_ZC` requests *may* still notify (`IORING_CQE_F_MORE` is a maybe), a chunk can be retained until the ring is torn down; the pool accounts for that as an outstanding pin, and a pin that outlives the connection is returned when the ring completes, not at close.

Everything else that looks phase-2 — multishot recv, provided buffers — is **excluded from every phase**: the contract hands reads to the application, and provided buffers would require the pool to hand storage to the kernel before the application asks for it. The v1 read path is one non-blocking `recv` per readiness event on every platform, and that is the stated price of the boundary.

---

## Appendix — platform facts relied on

| Fact | Source |
|---|---|
| kqueue is level-triggered; `EV_CLEAR` is the edge mode | [FreeBSD kqueue(2)](https://man.freebsd.org/cgi/man.cgi?query=kqueue&sektion=2) |
| `WSAPoll` never reported a failed connect before Windows 10 2004; it does since | [Microsoft](https://learn.microsoft.com/en-us/windows/win32/api/winsock2/nf-winsock2-wsapoll), [curl](https://daniel.haxx.se/blog/2012/10/10/wsapoll-is-broken/) |
| mio 1.x obtains readiness from `IOCTL_AFD_POLL` on `\Device\Afd`, completions via IOCP | [mio afd.rs](https://docs.rs/crate/mio/1.2.4/source/src/sys/windows/afd.rs) |
| `GetQueuedCompletionStatusEx` returns a batch in one call | [Microsoft](https://learn.microsoft.com/en-us/windows/win32/fileio/getqueuedcompletionstatusex-func) |
| `SEND_ZC` may produce a second CQE flagged `IORING_CQE_F_NOTIF`; errored requests **may** also notify, so check `F_MORE` rather than the result | [io_uring_enter(2)](https://manpages.debian.org/unstable/liburing-dev/io_uring_enter.2.en.html) |
| `IORING_OP_POLL_ADD` exists on Linux (readiness via a completion channel) | [io_uring_enter(2)](https://manpages.debian.org/unstable/liburing-dev/io_uring_enter.2.en.html) |
| Emulated readiness can be spurious; callers must handle `EAGAIN` | [libuv poll](https://docs.libuv.org/en/v1.x/poll.html) |
| Received ECN is available only where `recvmsg` exposes control messages — Linux `IP_RECVTOS`/`IPV6_RECVTCLASS`; Windows via `WSARecvMsg`; macOS/BSD varies by socket family | platform socket documentation; reported as `None` where absent |
| A datagram larger than the receive buffer is truncated by the kernel, and the reported length is the truncated one; `MSG_TRUNC` behaviour differs by platform | the design therefore treats `len == bytes.len()` as "possibly truncated" and never as an error |
| A zero-byte overlapped `WSASend` completes immediately with `SO_SNDBUF` full | empirical only, not vendor-documented; treated as a constraint ([report](https://stackoverflow.com/posts/42019668/timeline)) |
