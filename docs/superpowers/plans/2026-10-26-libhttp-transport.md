# libhttp transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the transport subsystem exactly as `docs/superpowers/specs/2026-10-26-libhttp-transport-design.md` defines it: a portable level-triggered readiness reactor with pooled chunks, credit sinks, per-kind timers, datagrams and three backends.

**Architecture:** `Driver` owns an authoritative table of sockets and one timer heap; backends (epoll, kqueue, AFD/IOCP) implement one internal `Backend` trait; the public surface is `Io` events, `Chunk` handles and generational ids. No thread is spawned and no operation is queued.

**Tech Stack:** Rust 2024 (MSRV 1.85), `libc` on Unix, `windows-sys` on Windows, `std` for addresses and time only.

**Spec:** `docs/superpowers/specs/2026-10-26-libhttp-transport-design.md`. Read §2–§7 before starting; the plan implements those contracts and adds nothing to them.

## Global Constraints

- Edition 2024, MSRV 1.85 (`rust-version = "1.85"`).
- No `async`, `Future`, `Waker`, executor, or thread spawning in the crate.
- Dependencies: `libc` (unix), `windows-sys` (windows). Nothing else. No `mio`, `tokio`, `bytes`, `httparse`.
- The public API never exposes a file descriptor, `epoll`, `io_uring` or `IoSlice`.
- Readiness is level-triggered on every platform; `recv`/`send` never block.
- Every chunk allocation is counted; `ChunkPool::get` fails at the cap and never allocates past it.
- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` pass on every commit.

## Review Focus

Five failure modes the spec implies but no happy path exercises. Each has a named test in the owning task.

1. **Spurious readiness.** `recv` after `Readable` may return `WouldBlock`; it must be an ordinary result, never an error. Task 8.
2. **Stale id after reuse.** An id for a closed connection must return `StaleId` even after its slot is reused. Tasks 1 and 6.
3. **Pool exhaustion.** A full pool must pause reads and stay quiescent, never spin. Task 11.
4. **Partial send.** `Sent::Partial(n)` reports bytes written and the caller retries the remainder; zero written is `WouldBlock`, not an error. Task 8.
5. **EOF latch.** The first `Eof` produces exactly one `HalfClose` and no further `Readable`. Task 8.

---

## File Structure

```
Cargo.toml
src/lib.rs                 crate root, re-exports, module wiring
src/error.rs               Error, ConnectError
src/id.rs                  RawId, IdTable<T>, ConnId, ListenerId, UdpId
src/chunk.rs               Storage, ChunkMut, Chunk, ChunkPool, PoolHandle, SliceError
src/credit.rs              CreditSink, CreditBatch
src/io/mod.rs              Config, PoolConfig, Interest, Read, Sent, TimerKind, CloseReason, Datagram, Ecn, BackendPath
src/io/driver.rs           Driver: table, interest, operations, event delivery
src/io/timer.rs            TimerHeap: slots, generations, deadlines
src/io/socket.rs           Socket: non-blocking accept/connect/recv/send/shutdown
src/io/backend.rs          Backend trait, RawEvent, WakeHandle internals
src/io/epoll.rs            Linux backend
src/io/kqueue.rs           macOS/BSD backend
src/io/windows.rs          AFD backend, self-test, WSAPoll fallback
tests/transport.rs         integration suite, shared by all platforms
.github/workflows/ci.yml   linux/macos/windows matrix
```

`socket.rs` is the only file that makes socket syscalls; `backend.rs` is the only platform abstraction; `driver.rs` holds all policy; `chunk.rs` is the only allocator.

---

### Task 1: Crate skeleton, errors, generational ids

**Files:**
- Create: `Cargo.toml`, `src/lib.rs`, `src/error.rs`, `src/id.rs`, `.github/workflows/ci.yml`
- Test: inline `#[cfg(test)] mod tests` in `src/id.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `error::Error` (`StaleId`, `Exhausted`, `NotActivated`, `HalfClosed`, `Io(std::io::Error)`, `Connect(ConnectError)`, `#[non_exhaustive]`), `error::ConnectError` (`Refused`, `Unreachable`, `TimedOut`, `Other(i32)`); `id::RawId { index: u32, generation: u32 }`; `id::ConnId`, `id::ListenerId`, `id::UdpId` (newtypes over `RawId`, `Copy + Eq + Hash + Ord + Debug`, `From<RawId>`); `id::IdTable<T>` with `insert`, `get`, `get_mut`, `remove`, `contains`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn stale_id_after_reuse_returns_none() {
    let mut t = IdTable::new();
    let a = t.insert("a");
    t.remove(a);
    let b = t.insert("b");                 // same slot, new generation
    assert_eq!(a.index(), b.index());
    assert_ne!(a.generation(), b.generation());
    assert_eq!(t.get(a), None);            // error path, not a panic
    assert_eq!(t.get(b), Some(&"b"));
}
```

- [ ] **Step 2: Run it** — `cargo test --lib id::tests` — Expected: FAIL, `IdTable` not found.

- [ ] **Step 3: Implement**

`Cargo.toml`: `edition = "2024"`, `rust-version = "1.85"`, `libc = "0.2"` under `[target.'cfg(unix)'.dependencies]`, `windows-sys` (features `Win32_Foundation`, `Win32_Networking_WinSock`, `Win32_Storage_FileSystem`, `Win32_System_IO`, `Win32_System_Threading`) under `[target.'cfg(windows)'.dependencies]`.

`IdTable<T>`: `Vec<Slot<T>>` + `free: Vec<u32>`; a freed slot keeps its index and bumps `generation` on reuse; `get` compares generations.

- [ ] **Step 4: Add CI and commit**

CI matrix on `ubuntu-latest`, `macos-latest`, `windows-latest` running fmt, clippy, test.
Run: `cargo test --lib && cargo clippy --all-targets -- -D warnings` — PASS. Commit `feat: crate skeleton, errors and generational ids`.

---

### Task 2: Public types

**Files:**
- Create: `src/io/mod.rs` (types only), `src/lib.rs` (re-exports)
- Test: inline in `src/io/mod.rs`

**Interfaces:**
- Consumes: `id::*`.
- Produces: `Config { pool: PoolConfig, batch_budget: usize }` and `PoolConfig { cap_bytes: usize, classes: &'static [usize] }` with `Default` (64 MiB, `&[4096, 17408, 66560]`, 512); `Interest` with `READ`, `WRITE`, `contains`, `is_empty`, `BitOr`; `Read { Data(usize), WouldBlock, Eof }`; `Sent { All, Partial(usize), WouldBlock }`; `TimerKind { Idle, ReadProgress, WriteProgress, Drain, HalfClose, Worker { exchange: u64 } }` (`#[non_exhaustive]`); `CloseReason { Local, Peer, PeerReset, Io(i32) }`; `Datagram<'a> { bytes: &'a mut [u8], len: usize, peer: SocketAddr, ecn: Option<Ecn> }`; `OutDatagram<'a> { bytes: &'a [u8], peer: SocketAddr, ecn: Option<Ecn> }`; `Ecn(pub u8)`; `BackendPath { Epoll, Kqueue, Afd, WsaPoll }`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn interest_composes_and_reports() {
    let i = Interest::READ | Interest::WRITE;
    assert!(i.contains(Interest::READ) && i.contains(Interest::WRITE));
    assert!(Interest(0).is_empty() == false || Interest::READ.is_empty() == false);
}
```

(Replace the loose assertion with `assert!(!Interest::READ.is_empty())` when implementing the empty mask.)

- [ ] **Step 2: Run** — FAIL. **Step 3: Implement** the types exactly as §2 of the spec, with `#[non_exhaustive]` on `Error`, `ConnectError`, `TimerKind`. **Step 4:** `cargo test --lib` PASS, commit `feat: public transport types`.

---

### Task 3: Chunks and pool

**Files:**
- Create: `src/chunk.rs`
- Test: inline

**Interfaces:**
- Consumes: `error::Error`, `io::PoolConfig`.
- Produces: `ChunkMut` (`bytes_mut`, `len`, `truncate`, `freeze`); `Chunk` (`len`, `slice(Range) -> Result<Chunk, SliceError>`, `as_ptr`, `Deref<[u8]>`, `Clone`, `Send`, `!Sync`); `SliceError::OutOfRange`; `ChunkPool::new`, `get(&self, usize) -> Result<ChunkMut, Error>`, `free_bytes`, `cap_bytes`; `PoolHandle` (`Clone + Send + Sync`, `get`, `free_bytes`). Constants `HEADROOM = 16`, `TAILROOM = 2`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn slice_shares_storage_and_keeps_the_range() {
    let pool = ChunkPool::new(PoolConfig::default());
    let mut m = pool.get(32).unwrap();
    m.bytes_mut()[..4].copy_from_slice(b"body");
    m.truncate(4);
    let c = m.freeze();
    let s = c.slice(1..4).unwrap();
    assert_eq!(&*s, b"ody");
    assert_eq!(s.as_ptr(), c.as_ptr().wrapping_add(1));   // no copy
}

#[test]
fn get_fails_at_the_cap_and_recovers_on_drop() {
    let pool = ChunkPool::new(PoolConfig { cap_bytes: 8192, classes: &[4096] });
    let a = pool.get(4096).unwrap();
    let b = pool.get(4096).unwrap();
    assert!(matches!(pool.get(4096), Err(Error::Exhausted)));
    drop(a); drop(b);
    assert_eq!(pool.free_bytes(), 8192);
    assert!(pool.get(4096).is_ok());
}
```

- [ ] **Step 2: Run** — FAIL. **Step 3: Implement**

`Storage` holds the block, class, and `Arc<PoolInner>`. `ChunkMut` is unique until `freeze`, which converts to `Arc<Storage>`-backed `Chunk` with a range. The refcount is `AtomicUsize` on every path. HEADROOM/TAILROOM are added on top of the class's usable size. `get` takes a thread-local freelist, then the shared `Mutex<Vec<usize>>`, then allocates under the cap; cross-thread drops push onto the shared list and call `wake_pump()` once per batch.

- [ ] **Step 4:** run PASS, commit `feat: pooled chunks with headroom and an atomic refcount`.

---

### Task 4: Credit sinks

**Files:**
- Create: `src/credit.rs`
- Test: inline

**Interfaces:**
- Consumes: `id::ConnId`.
- Produces: `CreditSink` (`credit(stream: u64, bytes: u32)`, `drain() -> CreditBatch`, `is_closed()`, `close()`, `Send + Sync`); `CreditBatch` (`iter() -> impl Iterator<Item = (u64, u32)>`, `is_empty`, `clear`).

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn drain_aggregates_and_clears() {
    let s = CreditSink::new();
    s.credit(1, 10); s.credit(1, 5); s.credit(3, 2);
    let b = s.drain();
    assert_eq!(b.iter().collect::<Vec<_>>(), vec![(1, 15), (3, 2)]);
    assert!(s.drain().is_empty());
}

#[test]
fn closed_sink_discards_late_credit() {
    let s = CreditSink::new();
    s.close();
    s.credit(1, 10);
    assert!(s.is_closed());
    assert!(s.drain().is_empty());
}
```

- [ ] **Step 2: Run** — FAIL. **Step 3: Implement** an `AtomicBool` closed flag plus a `Mutex<Vec<(u64, u32)>>` inbox (documented as a lock on the contended path; the design's hole-free requirement is a batched preallocated slab, which is a builder optimisation). **Step 4:** PASS, commit `feat: credit sinks that survive close`.

---

### Task 5: Backend trait, socket, epoll

**Files:**
- Create: `src/io/backend.rs`, `src/io/socket.rs`, `src/io/epoll.rs`
- Test: inline in `src/io/epoll.rs`

**Interfaces:**
- Consumes: `Interest`.
- Produces: `Socket` (`bind`, `local_addr`, `connect`, `accept`, `recv`, `send`, `shutdown_write`, `set_nonblocking`, `close`); `RawEvent { key: u64, readable: bool, writable: bool, error: bool, hangup: bool }`; `trait Backend { register, reregister, deregister, wait(Option<Duration>), next() -> Option<RawEvent>, wake() }`; `EpollBackend::new()`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn readiness_is_level_triggered_until_drained() {
    let (mut a, mut b) = std::os::unix::net::UnixStream::pair().unwrap();
    let mut be = EpollBackend::new().unwrap();
    be.register(&Socket::from_fd(a.as_raw_fd()), Interest::READ, 7).unwrap();
    std::io::Write::write_all(&mut b, b"x").unwrap();
    be.wait(Some(std::time::Duration::from_millis(100))).unwrap();
    assert_eq!(be.next().unwrap().key, 7);
    be.wait(Some(std::time::Duration::from_millis(100))).unwrap();
    assert_eq!(be.next().unwrap().key, 7);      // not drained: reported again
}
```

- [ ] **Step 2: Run** — FAIL. **Step 3: Implement** `epoll_create1(EPOLL_CLOEXEC)`, `epoll_ctl` only when the mask changes, `epoll_wait` into a fixed buffer, no `EPOLLET`, `wake` via a self-pipe or `eventfd` (not a dup of the epoll fd). **Step 4:** PASS, commit `feat: epoll backend, level-triggered`.

---

### Task 6: Driver table — listen, accept, activate, close

**Files:**
- Create: `src/io/driver.rs`
- Modify: `src/lib.rs`
- Test: `tests/transport.rs`

**Interfaces:**
- Consumes: Tasks 1–5.
- Produces: `Driver::new(Config)`, `listen`, `local_addr`, `unlisten`, `accept -> Result<Option<(ConnId, SocketAddr)>, Error>`, `activate`, `interest`, `close`, `place`, `next_io`, `wait`, `mark_active`, `active`, `drain_wakes`, `wake`, `wake_handle`, `pool`, `pool_handle`, `sink`, `backend_path`; and the `Io` enum.

```rust
pub enum Io {
    Acceptable(ListenerId), Connected(ConnId), ConnectFailed(ConnId, ConnectError),
    Readable(ConnId), Writable(ConnId), HalfClose(ConnId),
    Timer(ConnId, TimerKind), Closed(ConnId, CloseReason), Datagram(UdpId),
}
impl Clone for Io; impl std::fmt::Debug for Io;
```

- [ ] **Step 1: Write the failing tests** (with the shared helpers every later test uses)

```rust
fn driver() -> Driver { Driver::new(Config::default()).unwrap() }
fn listening(d: &mut Driver) -> (ListenerId, SocketAddr) {
    let l = d.listen("127.0.0.1:0".parse().unwrap()).unwrap();
    (l, d.local_addr(l).unwrap())
}
fn accept_one(d: &mut Driver, l: ListenerId) -> ConnId {
    loop {
        d.wait().unwrap();
        while let Some(io) = d.next_io() {
            if matches!(io, Io::Acceptable(_)) {
                let (c, _) = d.accept(l).unwrap().unwrap();
                d.activate(c).unwrap();
                return c;
            }
        }
    }
}

#[test]
fn close_delivers_exactly_one_closed_and_it_is_last() {
    let mut d = driver();
    let (l, addr) = listening(&mut d);
    let _peer = std::net::TcpStream::connect(addr).unwrap();
    let c = accept_one(&mut d, l);
    d.close(c);
    let mut seen = Vec::new();
    for _ in 0..2 { d.wait().unwrap(); while let Some(io) = d.next_io() { seen.push(io); } }
    assert!(matches!(seen.last(), Some(Io::Closed(id, _)) if *id == c));
    assert_eq!(seen.iter().filter(|e| matches!(e, Io::Closed(id, _) if *id == c)).count(), 1);
}

#[test]
fn accepted_socket_is_inactive_until_activate() {
    let mut d = driver();
    let (l, addr) = listening(&mut d);
    let mut peer = std::net::TcpStream::connect(addr).unwrap();
    peer.write_all(b"early").unwrap();
    d.wait().unwrap();
    let mut c = None;
    while let Some(io) = d.next_io() { if matches!(io, Io::Acceptable(_)) { c = Some(d.accept(l).unwrap().unwrap().0); } }
    let c = c.unwrap();
    d.wait().unwrap();
    while let Some(io) = d.next_io() { assert!(!matches!(io, Io::Readable(id) if id == c)); }
    d.activate(c).unwrap();
    let mut chunk = d.pool().get(64).unwrap();
    assert!(matches!(d.recv_pending(c, &mut chunk), Ok(Read::Data(5))));
}
```

(`recv_pending` is `recv` without the activation check, used only to prove the socket is live but unreported; implement it as `pub(crate)`.)

- [ ] **Step 2: Run** — FAIL. **Step 3: Implement**

`Driver` holds `IdTable<Conn>`, `IdTable<Listener>`, the backend, a `Vec<Io>` queue, a live set, `active: Vec<ConnId>`, and the pool. `accept` inserts with an empty mask; `activate` sets READ. `close` marks dead, purges timers, deregisters, and pushes `Closed` last. `next_io` filters every queued event against the live set before yielding.

- [ ] **Step 4:** PASS, commit `feat: driver table, listen/accept/activate/close`.

---

### Task 7: Connect with a timeout

**Files:**
- Modify: `src/io/driver.rs`
- Test: `tests/transport.rs`

**Interfaces:**
- Consumes: Task 6.
- Produces: `Driver::connect(addr, timeout: Duration) -> Result<ConnId, Error>`; `Io::Connected`, `Io::ConnectFailed(ConnId, ConnectError)`; an internal connect timeout slot.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn connect_success_emits_connected_once() {
    let peer = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut d = driver();
    let c = d.connect(peer.local_addr().unwrap(), Duration::from_secs(2)).unwrap();
    let mut n = 0;
    for _ in 0..64 {
        d.wait().unwrap();
        while let Some(io) = d.next_io() { if matches!(io, Io::Connected(id) if id == c) { n += 1; } }
        if n > 0 { break; }
    }
    assert_eq!(n, 1);
}

#[test]
fn connect_refused_emits_connect_failed() {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    let mut d = driver();
    let c = d.connect(addr, Duration::from_secs(2)).unwrap();
    let mut failed = false;
    for _ in 0..64 {
        d.wait().unwrap();
        while let Some(io) = d.next_io() {
            if matches!(io, Io::ConnectFailed(id, ConnectError::Refused) if id == c) { failed = true; }
        }
        if failed { break; }
    }
    assert!(failed);
}

#[test]
fn connect_timeout_is_reported_and_the_id_dies() {
    // 10.255.255.1 is a black hole on loopback-less CI; accept TimedOut from the socket too
    let mut d = driver();
    let c = d.connect("10.255.255.1:9".parse().unwrap(), Duration::from_millis(50)).unwrap();
    let mut timed_out = false;
    for _ in 0..64 {
        d.wait().unwrap();
        while let Some(io) = d.next_io() {
            if matches!(io, Io::ConnectFailed(id, ConnectError::TimedOut) if id == c) { timed_out = true; }
        }
        if timed_out { break; }
    }
    assert!(timed_out);
}
```

- [ ] **Step 2: Run** — FAIL.

- [ ] **Step 3: Implement**

`connect` creates a non-blocking socket, calls `connect`, registers WRITE, and arms a connect deadline in the timer heap. `Writable` → `take_error()` decides `Connected` or `ConnectFailed`; the mask returns to the application's (empty until `activate`). The deadline firing while connecting produces `ConnectFailed(TimedOut)` and closes the socket. `recv`/`send` on a connecting id return `NotActivated`/`HalfClosed`.

- [ ] **Step 4:** PASS, commit `feat: bounded non-blocking connect`.

---

### Task 8: recv, send, partial writes, EOF latch

**Files:**
- Modify: `src/io/driver.rs`
- Test: `tests/transport.rs`

**Interfaces:**
- Consumes: Task 7.
- Produces: `recv(&mut self, ConnId, &mut ChunkMut) -> Result<Read, Error>`; `send(&mut self, ConnId, &[Chunk]) -> Result<Sent, Error>`; `shutdown_write`; the read-shut latch and `Io::HalfClose`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn recv_returns_would_block_not_an_error_when_the_event_was_spurious() {
    let mut d = driver();
    let (l, addr) = listening(&mut d);
    let mut peer = std::net::TcpStream::connect(addr).unwrap();
    peer.write_all(b"hello").unwrap();
    let c = accept_one(&mut d, l);
    let mut chunk = d.pool().get(64).unwrap();
    assert!(matches!(d.recv(c, &mut chunk).unwrap(), Read::Data(5)));
    assert!(matches!(d.recv(c, &mut chunk).unwrap(), Read::WouldBlock));
}

#[test]
fn eof_latches_one_half_close_and_no_more_readable() {
    let mut d = driver();
    let (l, addr) = listening(&mut d);
    let mut peer = std::net::TcpStream::connect(addr).unwrap();
    let c = accept_one(&mut d, l);
    drop(peer);                                     // FIN
    let mut half = 0;
    let mut readables_after = 0;
    let mut closed = false;
    for _ in 0..8 {
        d.wait().unwrap();
        while let Some(io) = d.next_io() {
            match io {
                Io::HalfClose(id) if id == c => half += 1,
                Io::Readable(id) if id == c && half > 0 => readables_after += 1,
                Io::Closed(id, _) if id == c => closed = true,
                _ => {}
            }
        }
        if half > 0 { break; }
    }
    assert_eq!(half, 1);
    assert_eq!(readables_after, 0);
    assert!(!closed);                               // the write side is still open
}

#[test]
fn big_send_is_partial_and_never_claims_all() {
    let mut d = driver();
    let (l, addr) = listening(&mut d);
    let _peer = std::net::TcpStream::connect(addr).unwrap();   // never reads
    let c = accept_one(&mut d, l);
    let whole = d.pool().get(1 << 20).unwrap().freeze();
    match d.send(c, &[whole]).unwrap() {
        Sent::Partial(n) => assert!(n < 1 << 20),
        Sent::WouldBlock => {}
        Sent::All => panic!("1 MiB cannot fit a default socket buffer"),
    }
}
```

- [ ] **Step 2: Run** — FAIL.

- [ ] **Step 3: Implement**

`recv` calls `recv(2)` once: `0` → latch + `Read::Eof`; `EAGAIN` → `WouldBlock`. The latch makes `next_io` emit `HalfClose` once and suppress `Readable` for that id; `activate` clears it. `send` loops over the chain calling `send(2)` with `MSG_NOSIGNAL`, summing bytes; `EAGAIN` on the first chunk is `WouldBlock`. `shutdown_write` calls `shutdown(SHUT_WR)` and is idempotent; `send` after it is `HalfClosed`.

- [ ] **Step 4:** PASS, commit `feat: recv, partial send and the EOF latch`.

---

### Task 9: Timers

**Files:**
- Create: `src/io/timer.rs`
- Modify: `src/io/driver.rs`
- Test: inline in `src/io/timer.rs` and `tests/transport.rs`

**Interfaces:**
- Consumes: Task 6.
- Produces: `Driver::deadline(ConnId, TimerKind, Duration)`, `cancel_deadline`; `Io::Timer(ConnId, TimerKind)`; `TimerHeap` internal (`set`, `clear`, `next_deadline`, `pop_due(now) -> Vec<(ConnId, TimerKind, u64)>`, `purge(ConnId)`).

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn rearming_one_kind_does_not_disturb_another() {
    let mut h = TimerHeap::new();
    let c = ConnId::from(RawId { index: 0, generation: 0 });
    let t0 = Instant::now();
    h.set(c, TimerKind::Idle, t0 + Duration::from_secs(5));
    h.set(c, TimerKind::WriteProgress, t0 + Duration::from_secs(1));
    h.set(c, TimerKind::Idle, t0 + Duration::from_secs(9));
    assert_eq!(h.next_deadline(), Some(t0 + Duration::from_secs(1)));
}

#[test]
fn a_rearmed_slot_carries_a_new_generation() {
    let mut h = TimerHeap::new();
    let c = ConnId::from(RawId { index: 1, generation: 0 });
    let t0 = Instant::now();
    h.set(c, TimerKind::Drain, t0);
    let first = h.pop_due(t0).pop().unwrap();
    h.set(c, TimerKind::Drain, t0 + Duration::from_millis(50));
    assert_ne!(h.generation(c, TimerKind::Drain), first.2);
}

#[test]
fn purge_leaves_nothing_to_fire() {
    let mut h = TimerHeap::new();
    let c = ConnId::from(RawId { index: 2, generation: 0 });
    h.set(c, TimerKind::Idle, Instant::now());
    h.purge(c);
    assert!(h.pop_due(Instant::now() + Duration::from_secs(1)).is_empty());
}
```

- [ ] **Step 2: Run** — FAIL.

- [ ] **Step 3: Implement**

`BinaryHeap<Reverse<Entry>>` keyed by deadline, one slot per `(ConnId, TimerKind)` in a `HashMap` with a `generation: u64` bumped on every set and clear. `wait` derives its timeout from `next_deadline` and yields due entries as `Io::Timer`, dropping any whose generation changed. Two new tests in `tests/transport.rs` prove `Io::Timer` arrives and that a closed connection produces no timer afterwards.

- [ ] **Step 4:** PASS, commit `feat: per-kind timers with generation checks`.

---

### Task 10: Active set and wakes

**Files:**
- Modify: `src/io/driver.rs`, `src/io/backend.rs`
- Test: `tests/transport.rs`

**Interfaces:**
- Consumes: Task 9.
- Produces: `mark_active`, `active`, `drain_wakes`, `wake`, `wake_handle() -> WakeHandle`; `WakeHandle::wake(ConnId)`, `WakeHandle::wake_pump()`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn a_worker_wake_marks_the_connection_for_polling() {
    let mut d = driver();
    let (l, addr) = listening(&mut d);
    let _peer = std::net::TcpStream::connect(addr).unwrap();
    let c = accept_one(&mut d, l);
    let w = d.wake_handle();
    std::thread::spawn(move || w.wake(c));
    d.wait().unwrap();
    d.drain_wakes();
    assert!(d.active().any(|id| id == c));
}
```

- [ ] **Step 2: Run** — FAIL.

- [ ] **Step 3: Implement**

`WakeHandle` holds a clone of the backend's wake primitive (`Clone + Send + Sync`), a `Mutex<Vec<ConnId>>` inbox, and an `AtomicBool` for `wake_pump`. `wake(c)` pushes and signals; `wait` signals on pump-only wakes, and `drain_wakes` folds the inbox into `active`, deduplicated, before the scan. `active` is cleared at the start of each `wait`.

- [ ] **Step 4:** PASS, commit `feat: active set and cross-thread wakes`.

---

### Task 11: Pool pause and resume

**Files:**
- Modify: `src/io/driver.rs`
- Test: `tests/transport.rs`

**Interfaces:**
- Consumes: Tasks 8, 10.
- Produces: `pause_reads(&mut self, ConnId)`, `paused() -> usize`; resume at `free_bytes() >= cap_bytes() / 2`.

- [ ] **Step 1: Write the failing tests**

```rust
fn paused_setup() -> (Driver, ConnId, Vec<ChunkMut>) {
    let mut d = Driver::new(Config { pool: PoolConfig { cap_bytes: 4096, classes: &[4096] }, ..Default::default() }).unwrap();
    let (l, addr) = listening(&mut d);
    let mut peer = std::net::TcpStream::connect(addr).unwrap();
    let c = accept_one(&mut d, l);
    let held = vec![d.pool_handle().get(4096).unwrap()];      // the pool is now full
    peer.write_all(&[0u8; 4096]).unwrap();
    d.wait().unwrap();
    while let Some(io) = d.next_io() {
        if matches!(io, Io::Readable(id) if id == c) {
            assert!(matches!(d.pool_handle().get(4096), Err(Error::Exhausted)));
            d.pause_reads(c);
        }
    }
    assert_eq!(d.paused(), 1);
    (d, c, held)
}

#[test]
fn paused_connection_is_quiescent() {
    let (mut d, c, _held) = paused_setup();
    let t = Instant::now();
    d.wait().unwrap();
    assert!(t.elapsed() < Duration::from_millis(50));
    while let Some(io) = d.next_io() { assert!(!matches!(io, Io::Readable(id) if id == c)); }
}

#[test]
fn returning_chunks_resumes_reads() {
    let (mut d, c, held) = paused_setup();
    drop(held);                                                // crosses the watermark
    let mut readable = false;
    for _ in 0..8 {
        d.wait().unwrap();
        while let Some(io) = d.next_io() { if matches!(io, Io::Readable(id) if id == c) { readable = true; } }
        if readable { break; }
    }
    assert!(readable);
}
```

- [ ] **Step 2: Run** — FAIL.

- [ ] **Step 3: Implement**

`pause_reads` stores the application's current mask in the connection, clears READ, and parks the id. `next_io` filters `Readable` for parked ids, including events already queued. `wait` restores the stored mask once `free_bytes() >= cap_bytes() / 2`, in one pass. `pause_reads` on an already-paused or connecting id is a no-op.

- [ ] **Step 4:** PASS, commit `feat: pause reads when the pool is full`.

---

### Task 12: Datagrams

**Files:**
- Modify: `src/io/driver.rs`, `src/io/socket.rs`
- Test: `tests/transport.rs`

**Interfaces:**
- Consumes: Task 6.
- Produces: `bind_udp`, `udp_local_addr`, `close_udp`, `recv_datagrams(&mut self, UdpId, &mut [Datagram]) -> Result<Read, Error>`, `send_datagrams(&mut self, UdpId, &[OutDatagram]) -> Result<usize, Error>`, `Io::Datagram(UdpId)`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn datagrams_round_trip_with_peer_addresses() {
    let mut d = driver();
    let a = d.bind_udp("127.0.0.1:0".parse().unwrap()).unwrap();
    let b = d.bind_udp("127.0.0.1:0".parse().unwrap()).unwrap();
    let pa = d.udp_local_addr(a).unwrap();
    let pb = d.udp_local_addr(b).unwrap();
    assert_eq!(d.send_datagrams(a, &[
        OutDatagram { bytes: b"one", peer: pb, ecn: None },
        OutDatagram { bytes: b"two", peer: pb, ecn: None },
    ]).unwrap(), 2);

    let mut b1 = [0u8; 64]; let mut b2 = [0u8; 64];
    let mut out = [Datagram { bytes: &mut b1, len: 0, peer: pa, ecn: None },
                   Datagram { bytes: &mut b2, len: 0, peer: pa, ecn: None }];
    let mut n = 0;
    for _ in 0..64 {
        d.wait().unwrap();
        while let Some(io) = d.next_io() { if matches!(io, Io::Datagram(id) if id == b) { n = d.recv_datagrams(b, &mut out).unwrap(); } }
        if n > 0 { break; }
    }
    assert_eq!(&out[0].bytes[..out[0].len], b"one");
    assert_eq!(out[0].peer, pa);
}
```

Plus: a test that an oversized datagram sets `len == bytes.len()` and is not an error, and one that `send_datagrams` returns a prefix count when the kernel buffer is full.

- [ ] **Step 2: Run** — FAIL.

- [ ] **Step 3: Implement** `recvmmsg`/`recvfrom` and `sendmmsg`/`sendto` behind `readiness = Datagram`; `ecn` from `IP_RECVTOS`/`IPV6_RECVTCLASS` where available, else `None`; `recv_datagrams` returns `Read::Data(n)` or `WouldBlock`; `send_datagrams` returns the prefix count and enables write readiness while a short send is outstanding.

- [ ] **Step 4:** PASS, commit `feat: batched datagram sockets`.

---

### Task 13: kqueue backend

**Files:**
- Create: `src/io/kqueue.rs`
- Modify: `src/io/mod.rs`, `.github/workflows/ci.yml`
- Test: `tests/transport.rs` on macOS

**Interfaces:**
- Produces: `KqueueBackend` implementing `Backend`.

- [ ] **Step 1:** Run the full `tests/transport.rs` suite on macOS with `src/io/mod.rs` selecting `kqueue.rs` under `cfg(any(target_os = "macos", target_os = "freebsd"))`. Expected: FAIL, `KqueueBackend` not found.

- [ ] **Step 2: Implement** `kqueue()`; one `EVFILT_READ` and one `EVFILT_WRITE` kevent per interest, `udata` = key; `EV_ADD | EV_ENABLE` and `EV_DELETE`; **no `EV_CLEAR`** (level-triggered is the default and the suite depends on it); `wake` via `EVFILT_USER`.

- [ ] **Step 3:** `cargo test --test transport` PASS, commit `feat: kqueue backend`.

---

### Task 14: Windows AFD backend, self-test, fallback

**Files:**
- Create: `src/io/windows.rs`
- Modify: `src/io/mod.rs`, `.github/workflows/ci.yml`
- Test: `tests/transport.rs` on Windows, inline self-test

**Interfaces:**
- Produces: `AfdBackend`; `fn afd_self_test() -> Result<(), AfdUnavailable>`; `backends` selected at startup.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn afd_reports_again_after_an_undrained_completion() {
    // loopback TCP pair; arm AFD poll for read; peer writes 1 byte;
    // observe read readiness; do NOT read; re-arm; observe it again.
}
```

- [ ] **Step 2: Run** — `cargo test --test transport afd -- --nocapture` — FAIL.

- [ ] **Step 3: Implement** `\Device\Afd` + `NtDeviceIoControlFile(IOCTL_AFD_POLL)` per socket, completions drained with `GetQueuedCompletionStatusEx`, one poll re-issued per completion. `afd_self_test` runs a loopback pair through read, write, peer abort (`PeerReset`), and the undrained re-report above; on any mismatch it returns `AfdUnavailable` and the driver uses `WSAPoll` for read/write plus `ConnectEx` for connect. `backend_path()` reports which is live.

- [ ] **Step 4:** PASS, commit `feat: Windows AFD backend with self-test and fallback`.

---

## Self-review notes

- Credit is not in this plan: no transport code consumes it. The `CreditSink` exists so `conn` can, which is the next plan.
- `send_zc` is M4 and is not here; its public shape is fixed in the spec (§9) so adding it changes no type.
- Deferred deliberately: multi-datagram batching (one per call in v1), provided-buffer/multishot reads (excluded in every phase, spec §9).

