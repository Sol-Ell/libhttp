# libhttp transport — Design

**Status:** subsystem design, written for verification. It replaces §6 and §7 of the previous single-document design; the ones that follow are `h1`, `h2`, `h3`, `tls`, `dispatch`, `proxy`.
**Parent:** `2026-10-26-libhttp-design.md` — intent, goals, architecture, milestones, gates. This document does not restate them.

Everything here is normative. Where a decision is deferred, §9 says so and nothing points at it.

---

## 1. Scope

**Owns:** sockets, readiness, interest, timers, ids, datagrams, the chunk pool, credit sinks, close and half-close.

**Does not own:** HTTP (any version), TLS, worker dispatch, the pump loop (the application writes it). No HTTP type appears in this document, and no thread is spawned by any code in it.

---

## 2. Types

```rust
pub struct Config { pub pool: PoolConfig, pub batch_budget: usize }
// defaults: 64 MiB pool, 512 events per wait()

pub struct PoolConfig { pub cap_bytes: usize, pub classes: &'static [usize] }
// default classes are usable sizes: 4 KiB, 17 KiB, 65 KiB. Headroom and tailroom are added on top.

pub struct Interest(u8);           // READ | WRITE; an empty mask is legal and means "report nothing"

pub enum Read  { Data(usize), WouldBlock, Eof }
pub enum Sent  { All, Partial(usize), WouldBlock }
pub enum TimerKind { Idle, ReadProgress, WriteProgress, Drain, HalfClose, Worker }
pub enum CloseReason { Local, Peer, PeerReset, TimedOut(TimerKind), Io(i32) }

pub enum Error {
    StaleId,                       // a generational handle that is no longer live
    Exhausted,                     // the pool cap was reached
    NotActivated,                  // recv on a connection whose reads are not open
    Io(std::io::Error),
    Connect(ConnectError),
}

pub struct ConnectError { /* Refused | Unreachable | TimedOut | Other(i32), plus the OS code */ }

pub struct Datagram<'a>   { pub bytes: &'a mut [u8], pub len: usize, pub peer: SocketAddr, pub ecn: Option<Ecn> }
pub struct OutDatagram<'a>{ pub bytes: &'a [u8], pub peer: SocketAddr, pub ecn: Option<Ecn> }
pub struct Ecn(pub u8);
```

---

## 3. Driver surface

```rust
impl Driver {
    pub fn new(config: Config) -> Result<Driver, Error>;

    pub fn listen(&mut self, addr: SocketAddr) -> Result<ListenerId, Error>;
    pub fn local_addr(&self, l: ListenerId) -> Result<SocketAddr, Error>;
    pub fn connect(&mut self, addr: SocketAddr) -> Result<ConnId, Error>;
    pub fn accept(&mut self, l: ListenerId) -> Result<(ConnId, SocketAddr), Error>; // inactive
    pub fn activate(&mut self, c: ConnId) -> Result<(), Error>;
    pub fn interest(&mut self, c: ConnId, i: Interest) -> Result<(), Error>;

    pub fn recv(&mut self, c: ConnId, buf: &mut ChunkMut) -> Result<Read, Error>;
    pub fn send(&mut self, c: ConnId, chunks: &[Chunk]) -> Result<Sent, Error>;
    pub fn close(&mut self, c: ConnId);
    pub fn shutdown_write(&mut self, c: ConnId) -> Result<(), Error>;

    pub fn bind_udp(&mut self, addr: SocketAddr) -> Result<UdpId, Error>;
    pub fn udp_local_addr(&self, u: UdpId) -> Result<SocketAddr, Error>;
    pub fn recv_datagrams(&mut self, u: UdpId, out: &mut [Datagram]) -> Result<usize, Error>;
    pub fn send_datagrams(&mut self, u: UdpId, out: &[OutDatagram]) -> Result<usize, Error>;

    pub fn deadline(&mut self, c: ConnId, kind: TimerKind, after: Duration);
    pub fn cancel_deadline(&mut self, c: ConnId, kind: TimerKind);

    pub fn wait(&mut self) -> Result<(), Error>;
    pub fn next_io(&mut self) -> Option<Io>;
    pub fn mark_active(&mut self, c: ConnId);
    pub fn active(&self) -> impl Iterator<Item = ConnId> + '_;
    pub fn wake(&self) -> Result<(), Error>;               // any thread, through WakeHandle
    pub fn pause_reads(&mut self, c: ConnId);
    pub fn paused(&self) -> usize;
    pub fn pool(&self) -> &ChunkPool;
}
```

`wake` is callable from any thread holding a `WakeHandle` (§6.3); every other method takes `&mut Driver` and is therefore confined to the pump thread.

---

## 4. Contracts

### 4.1 Readiness

Level-triggered on every platform: `wait` reports a socket while it has readiness to report, and the application never re-arms anything. Readiness is a hint — `recv` after `Readable` may return `WouldBlock`, which is an ordinary result and never an error.

`recv` and `send` are non-blocking by contract. The driver never parks the calling thread; the application waits once, in `wait`.

`wait` returns when an event is available, a timer is due, or `batch_budget` events have been produced. The budget bounds events, not time: one event can consume `READ_BUDGET` (256 KiB) plus decryption and parsing, so deadline lateness is bounded by `budget × per-event work` and is published as a measured number, not asserted.

### 4.2 Interest, activation, and the three ways reads stop

`interest(c, mask)` **replaces** the mask; it does not merge. The application owns the mask, with two stated exceptions:

1. **Acceptance.** `accept` returns an inactive connection with an empty mask. Nothing is reported for it until `activate`, so no id can arrive for a connection the application has not inserted. `recv` on an inactive connection returns `NotActivated`.
2. **The pool pause.** When `pool.get` fails, the application calls `pause_reads(c)`: the driver clears READ and parks the id. `wait` re-arms parked ids once `pool.free_bytes()` is at or above half the cap, in one pass on the pump thread. This is the only place the driver changes a mask the application set, and `paused()` makes it observable.

Without the pause, level-triggered readiness would re-report the same socket immediately and spin. With it, a full pool is a quiescent state, not a busy loop.

**Write interest is the application's**, including after a `Partial`: `Sent::Partial` and `Sent::WouldBlock` mean "keep the tail and set WRITE"; `Sent::All` on an empty outbox means "clear WRITE". The driver never flips it implicitly — one owner, no races.

### 4.3 EOF latch

The first `Read::Eof` on a connection sets a read-shut latch. From then on the driver reports `HalfClose` once and stops reporting `Readable` for that id, because a FIN'd socket is readable forever and level-triggered readiness would otherwise spin. `activate` clears the latch; a half-closed connection that still wants reads calls it again.

`HalfClose` is not `Closed`: a peer that half-closes leaves the connection writable, and conflating the two is how a relay tears down a tunnel that is still live.

### 4.4 Send and partial writes

`send` takes a **chain** and returns `Sent::{All, Partial(n), WouldBlock}` where `n` counts bytes across the chain in order.

- `All` — the whole chain is in the kernel.
- `Partial(n)` — `n` bytes written; the caller keeps the remainder (drop the first `n` bytes of the chain, keep the rest) and retries on `Writable`.
- `WouldBlock` — nothing written, not an error.

The driver holds no output queue. A short write is the application's to resume, and that is the whole reason `Sent` exists.

`send` on a closed or stale id is `Error::StaleId`, not a panic and not a silent success.

### 4.5 Close and half-close

- `close(c)` — abort. Remove the socket, purge its timers, mark its credit sink closed, deliver one `Closed`. It does **not** free storage the application holds; force-reclaiming would underflow the refcount when the application's copy drops.
- `shutdown_write(c)` — `shutdown(SHUT_WR)` on Unix, `shutdown(SD_SEND)` on Windows. Legal only when the application has no partial send outstanding; the driver cannot detect the violation, which is why it is a precondition (§4.4) rather than a check. Calling it twice is a no-op.
- `Closed` is delivered exactly once and is the last event an id ever produces. Any event already queued for that id in the same batch is filtered out before delivery — including a `Writable` that preceded the close.

### 4.6 Timers

One heap, one clock, one owner: the pump thread.

- `deadline(c, kind, after)` sets that **(connection, kind)** slot. Re-arming one kind does not disturb another.
- `cancel_deadline(c, kind)` clears one slot.
- `wait` derives its timeout from the earliest slot; no call site passes a timeout.
- Each slot carries a generation bumped on every set or cancel. At delivery the driver compares generations and drops stale events, so a re-arm between `wait` and the drain cannot fire an old deadline.
- `close` purges every slot for the connection.

`TimerKind::Worker` exists so a dispatched exchange's deadline lives in the same heap as everything else; the pump fires it and wakes the parked worker (§`dispatch`).

### 4.7 Datagrams

`bind_udp`, `recv_datagrams`, `send_datagrams` are batched. A datagram read fills `len`, `peer`, and `ecn` where the platform genuinely provides it; otherwise `ecn` is `None`. Nothing is fabricated, and §Appendix lists what each platform actually supplies.

`recv_datagrams` returns `0` on `WouldBlock`, matching `Read`. `send_datagrams` returns how many were accepted; a partially accepted batch is legal and the caller retries from the returned index.

h3 application datagrams (RFC 9221) are **not** this surface. They travel inside a QUIC connection and are the `h3` document's concern; this one only moves UDP packets.

### 4.8 Ids

`ConnId`, `ListenerId`, `UdpId` are `(index, generation)` handles. A stale handle returns `Error::StaleId`; it never aliases a new object and never panics, including after the slot is reused.

### 4.9 The active set

`mark_active(c)` records that the application has work for `c` this iteration — after feeding bytes, after producing output, after a deadline, after a wakeup. `active()` yields exactly those ids so the loop need not scan every connection.

The set belongs to the driver and is cleared at the start of each `wait`. Marking is not optional: a connection that is never marked is never polled. Its cost is one integer write per busy iteration.

---

## 5. Chunks, pool, credit

### 5.1 Chunk

- `ChunkMut` (from `pool.get`) is uniquely owned; `freeze()` converts it to a shared `Chunk`. Writing requires ownership, enforced by the type system.
- `Chunk: Clone + Send`, not `Sync`. `clone` and `slice(range)` share storage and bump the refcount — **atomic on every path**, because a clone can cross a thread while another stays. An uncontended increment is a lock-prefixed add; that is the cost.
- Dropping the last reference returns storage to the owning pool. Cross-thread returns batch into that pool's inbound queue, one wakeup per batch.
- Headroom and tailroom exist per *allocation*, not per *slice*: a read slice may begin at offset 0 with another stream's payload immediately before it, so headroom on the allocation is not headroom on the slice. Framing therefore uses a chain (§4.4), never a back-fill into shared storage. Headroom serves chunks the upper layers allocate for themselves.

### 5.2 Pool

`ChunkPool` is per shard, classed by usable size, and bounded by total bytes. `get(size)` takes from the class or allocates; at the cap it returns `Error::Exhausted` and allocates nothing. `free_bytes()` is exposed because the pause/resume rule (§4.2) needs it. The class sizes and the cap are `PoolConfig`; defaults are 4/17/65 KiB usable and 64 MiB per shard.

### 5.3 Credit sink

```rust
pub struct CreditSink { /* generation, closed flag, queue of (stream, bytes) */ }
pub fn sink(&self, c: ConnId) -> Arc<CreditSink>;
```

The transport provides the sink and its lifetime rules; the protocol decides what a credit means. A handle dropped after `close` credits a sink that knows it is dead, rather than a connection whose slot may have been reused. The pump drains sinks in `tick`, which belongs to the connection layer, not here.

---

## 6. Backends

One internal trait: `register`, `reregister`, `deregister`, `wait`, `next`, `wake`.

- **Linux:** epoll, level-triggered (never `EPOLLET`); `epoll_ctl` only when the mask changes.
- **macOS/BSD:** kqueue, `EVFILT_READ`/`EVFILT_WRITE`, level-triggered (no `EV_CLEAR`).
- **Windows:** `IOCTL_AFD_POLL` on `\Device\Afd`, completions through the same IOCP with `GetQueuedCompletionStatusEx`. AFD polls are one-shot, so the backend re-issues one after each completion. The documented fallback is `WSAPoll` for read/write readiness plus `ConnectEx` for connect, because `WSAPoll` never reports a failed connect.

### 6.1 Wake

`WakeHandle` is `Send + Sync`, holds a duplicated backend handle, and exposes `wake()` only. It exists so a worker can signal the pump without touching `&mut Driver`. `Driver::wake` is the same operation on the pump thread.

### 6.2 Startup self-test

Before the AFD backend is used, a socketpair exercises read, write, abort, and **re-reporting after an event was deliberately not drained**. Any mismatch selects the `WSAPoll` path instead. `Driver::backend_path()` reports which one is live, so every published number records it.

### 6.3 Phase 2

`io_uring` submission batching (datagrams, file bodies), `SEND_ZC`, splice/sendfile/`TransmitFile`. `send` itself stays a synchronous non-blocking syscall on every backend, because no platform offers a writability probe on a completion channel.

`SEND_ZC` needs one contract now so the type does not change later: it is a **separate operation** `send_zc(c, chunks) -> Sent`, where `All` means "submitted and retained", not "written". The chunks stay alive via the ring's reference until its notification CQE; the pool does not reclaim them on refcount zero during that window. `send` never returns `All` early.

---

## 7. Budget

| Item | Target | Note |
|---|---|---|
| Pool per shard | 64 MiB cap | the largest single line item at 8 shards: 512 MiB |
| Idle connection, no partial read | ≤ 1 KiB | parser and connection state only; no chunk retained |
| Partial request head held | one 17 KiB class chunk | a 16 KiB read lands in the second class |
| Per-event read budget | 256 KiB | bounds one socket's share of a `wait` |
| Batch budget | 512 events | bounds a wait's duration, not its fairness |

Copy budget for this layer: kernel → chunk inbound (1), chunk → kernel outbound (1), nothing else. Atomics: one allocation and one free per chunk, one per split.

---

## 8. Factors a builder decides

Constants only, no contract: the exact class sizes beyond the stated usable numbers, free-list growth policy, the ring depth of the backend's event buffer, and the syscall wrapper module layout.

Not theirs: readiness semantics, interest ownership (including the two exceptions), the EOF latch, `Sent`'s meaning, close versus half-close, timer ownership, id staleness, the pause rule, the pool cap.

---

## 9. Deferred, and where

Nothing in this document is deferred. The only phase-2 item is §6.3, and its public shape (`send_zc`) is fixed here.

---

## Appendix — platform facts relied on

| Fact | Source |
|---|---|
| kqueue is level-triggered; `EV_CLEAR` is the edge mode | [FreeBSD kqueue(2)](https://man.freebsd.org/cgi/man.cgi?query=kqueue&sektion=2) |
| `WSAPoll` marshals the array per call and never reports a failed connect | [Microsoft](https://learn.microsoft.com/en-us/windows/win32/api/winsock2/nf-winsock2-wsapoll), [curl](https://daniel.haxx.se/blog/2012/10/10/wsapoll-is-broken/) |
| mio 1.x and wepoll get readiness from `IOCTL_AFD_POLL`, completions via IOCP | [mio afd.rs](https://docs.rs/crate/mio/1.2.4/source/src/sys/windows/afd.rs) |
| `GetQueuedCompletionStatusEx` returns a batch in one call | [Microsoft](https://learn.microsoft.com/en-us/windows/win32/fileio/getqueuedcompletionstatusex-func) |
| `SEND_ZC` produces two CQEs; `F_NOTIF` means "buffer reusable"; errored requests still notify | [io_uring_enter(2)](https://manpages.debian.org/unstable/liburing-dev/io_uring_enter.2.en.html) |
| Multishot recv needs provided buffers; a per-call buffer fails `EINVAL` | [liburing 1331](https://github.com/axboe/liburing/issues/1331) |
| Emulated readiness can be spurious; callers must handle `EAGAIN` | [libuv poll](https://docs.libuv.org/en/v1.x/poll.html) |
| A zero-byte overlapped `WSASend` completes immediately with `SO_SNDBUF` full | empirical report only, treated as a constraint; not vendor-documented ([source](https://stackoverflow.com/posts/42019668/timeline)) |
