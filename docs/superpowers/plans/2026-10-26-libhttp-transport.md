# libhttp Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A portable, readiness-shaped socket reactor with pooled refcounted chunks, a single-owner timer heap and generational ids, behaving identically on Linux, macOS/BSD and Windows.

**Architecture:** `Driver` owns an authoritative table of sockets and timers. Three backends (epoll, kqueue, AFD/IOCP) implement one internal `Backend` trait; the public surface is only `Io` events, `Chunk` handles and ids. No thread is spawned, no operation is queued, and `recv`/`send` never block.

**Tech Stack:** Rust 2024 (MSRV 1.85), `libc` on Unix, `windows-sys` on Windows, `std` for addresses and time only.

**Spec:** `docs/superpowers/specs/2026-10-26-libhttp-design.md` (§5.2, §6, §7). Read §6.1–6.8 and §7 before starting; this plan implements exactly those contracts and nothing else.

## Global Constraints

- Edition 2024, MSRV 1.85 (`rust-version = "1.85"`).
- No `async`, `Future`, `Waker`, executor, or thread spawning in the crate.
- Dependencies: `libc` (unix), `windows-sys` (windows). Nothing else. No `mio`, `tokio`, `bytes`, `httparse`.
- The public API never exposes `fd`, `epoll`, `io_uring` or `IoSlice`.
- Readiness is level-triggered on every platform. `recv`/`send` are non-blocking by contract.
- Every allocation goes through the pool and is counted. `pool.get` fails at the cap; it never allocates past it.
- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` pass on every commit.

## Review Focus

Each line is a failure mode the spec implies but no task's happy path exercises. The owning task carries its test.

1. **Spurious readiness.** A `recv` after a `Readable` event may return `WouldBlock`. It must be an ordinary result — never an error, never a panic. Owning task: 6.
2. **Stale ids after reuse.** An id from a closed connection must return an error even after its slot is reused by a new connection. Owning task: 1 and 5.
3. **Pool exhaustion.** A full pool must pause reads, not spin. Owning task: 8.
4. **Partial send.** `send` reports the byte count; the caller keeps the remainder and retries. Zero written is not an error. Owning task: 7.
5. **Close ordering.** Nothing is delivered for an id after its `Closed` event, even if it was queued in the same batch. Owning task: 5.

---

## File Structure

```
Cargo.toml
src/lib.rs                     crate root, pub use, module wiring
src/error.rs                   Error, ConnectError, PoolExhausted
src/id.rs                      IdTable<T>, RawId, ConnId/ListenerId/UdpId
src/chunk.rs                   Storage, ChunkMut, Chunk, ChunkPool
src/io/mod.rs                  Interest, Io, Read, Sent, CloseReason, TimerKind
src/io/driver.rs               Driver: table, interest, ops, event delivery
src/io/timer.rs                TimerHeap: arm/deadline/pop_due/purge
src/io/socket.rs               Socket: non-blocking accept/connect/recv/send/shutdown
src/io/backend.rs              Backend trait, RawEvent
src/io/epoll.rs                Linux backend
src/io/kqueue.rs               macOS/BSD backend
src/io/windows.rs              AFD backend, self-test, WSAPoll fallback
tests/transport.rs             integration suite, shared by all platforms
.github/workflows/ci.yml       linux/macos/windows matrix
```

Responsibilities: `socket.rs` is the only file that calls socket syscalls; `backend.rs` is the only abstraction between platforms; `driver.rs` holds all policy (ids, interest, pause, event order); `chunk.rs` is the only allocator.

---

### Task 1: Crate skeleton, errors, generational ids

**Files:**
- Create: `Cargo.toml`, `src/lib.rs`, `src/error.rs`, `src/id.rs`, `.github/workflows/ci.yml`
- Test: inline `#[cfg(test)] mod tests` in `src/id.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `error::Error`, `error::ConnectError`, `error::PoolExhausted`; `id::RawId { index: u32, generation: u32 }`; `id::ConnId`, `id::ListenerId`, `id::UdpId` (newtypes over `RawId`, `Copy + Eq + Hash + Debug`); `id::IdTable<T>` with `insert(&mut self, T) -> RawId`, `get(&self, RawId) -> Option<&T>`, `get_mut(&mut self, RawId) -> Option<&mut T>`, `remove(&mut self, RawId) -> Option<T>`, `contains(&self, RawId) -> bool`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn stale_id_after_reuse_returns_none() {
    let mut t = IdTable::new();
    let a = t.insert("a");
    t.remove(a);
    let b = t.insert("b");           // same slot, new generation
    assert_eq!(a.index, b.index);
    assert_ne!(a.generation, b.generation);
    assert_eq!(t.get(a), None);      // stale: error path, not a panic
    assert_eq!(t.get(b), Some(&"b"));
}

#[test]
fn remove_twice_is_none_not_panic() {
    let mut t = IdTable::new();
    let a = t.insert(1u8);
    assert_eq!(t.remove(a), Some(1));
    assert_eq!(t.remove(a), None);
    assert_eq!(t.get(a), None);
}
```

- [ ] **Step 2: Run them**

Run: `cargo test --lib id::tests`
Expected: FAIL — `IdTable` not found.

- [ ] **Step 3: Implement**

`Cargo.toml`: `edition = "2024"`, `rust-version = "1.85"`, `[dependencies] libc = "0.2"` under `[target.'cfg(unix)'.dependencies]`, `windows-sys = { version = "0.59", features = ["Win32_Foundation", "Win32_Networking_WinSock", "Win32_Storage_FileSystem", "Win32_System_IO", "Win32_System_Threading"] }` under `[target.'cfg(windows)'.dependencies]`.

`IdTable<T>`: `Vec<Slot<T>>` + `free: Vec<u32>`; a freed slot keeps its index and bumps `generation` on reuse. `RawId` carries both. `get` compares generations and returns `None` on mismatch.

`src/error.rs`: concrete enums, `#[non_exhaustive]`, `impl std::error::Error`. `ConnectError { Refused, Unreachable, TimedOut, Other(i32) }`, `PoolExhausted` unit struct, `Error { StaleId, Io(std::io::Error), Connect(ConnectError), ListenFailed, NotReady }`.

CI workflow: matrix `ubuntu-latest`, `macos-latest`, `windows-latest`; `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`.

- [ ] **Step 4: Run again and commit**

Run: `cargo test --lib && cargo clippy --all-targets -- -D warnings`
Expected: PASS. Commit: `feat: crate skeleton, errors and generational ids`.

---

### Task 2: Chunks and the pool

**Files:**
- Create: `src/chunk.rs`
- Test: inline tests in `src/chunk.rs`

**Interfaces:**
- Consumes: `error::PoolExhausted`.
- Produces: `chunk::ChunkPool::new(PoolConfig) -> ChunkPool`; `PoolConfig { cap_bytes: usize, classes: &'static [usize] }`; `ChunkPool::get(&self, size: usize) -> Result<ChunkMut, PoolExhausted>`; `ChunkPool::free_bytes(&self) -> usize`; `ChunkMut::bytes_mut(&mut self) -> &mut [u8]`, `ChunkMut::truncate(&mut self, len: usize)`, `ChunkMut::freeze(self) -> Chunk`; `Chunk: Deref<Target = [u8]> + Clone + Send`; `Chunk::slice(&self, range: Range<usize>) -> Result<Chunk, SliceError>`; `const HEADROOM: usize = 16`, `const TAILROOM: usize = 2`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn slice_shares_storage_and_keeps_headroom() {
    let pool = ChunkPool::new(PoolConfig { cap_bytes: 1 << 20, classes: &[64] });
    let mut m = pool.get(32).unwrap();
    m.bytes_mut()[..4].copy_from_slice(b"body");
    m.truncate(4);
    let c = m.freeze();
    let s = c.slice(0..4).unwrap();
    assert_eq!(&*s, b"body");
    assert_eq!(c.slice(0..4).unwrap().as_ptr(), c.as_ptr()); // no copy
}

#[test]
fn get_fails_at_cap_and_does_not_overallocate() {
    let pool = ChunkPool::new(PoolConfig { cap_bytes: 1024, classes: &[256] });
    let held: Vec<_> = (0..4).map(|_| pool.get(256).unwrap()).collect();
    assert_eq!(pool.get(256), Err(PoolExhausted));  // 4 * 256 == cap
    assert_eq!(pool.free_bytes(), 0);
    drop(held);
    assert_eq!(pool.free_bytes(), 1024);
}

#[test]
fn byte_limit_is_the_cap_not_the_class_count() {
    // a 4 KiB cap with 2 KiB classes must not hand out 4 chunks of 4 KiB
    let pool = ChunkPool::new(PoolConfig { cap_bytes: 4096, classes: &[2048] });
    let _a = pool.get(2048).unwrap();
    let _b = pool.get(2048).unwrap();
    assert_eq!(pool.get(2048), Err(PoolExhausted));
}
```

- [ ] **Step 2: Run them**

Run: `cargo test --lib chunk::tests`
Expected: FAIL.

- [ ] **Step 3: Implement**

`Storage` = `{ block: *mut u8, len: usize, class: usize, pool: Arc<PoolInner> }`. `ChunkMut` holds a `Box<Storage>`-like unique handle until `freeze`, which converts it to `Arc<Storage>` shared by `Chunk`. Drop of the last `Arc` returns the block to the owning pool's free list, and if the current thread is not the owner, pushes it onto a `Mutex<Vec<usize>>` return queue and wakes the owner once per batch.

The 16-byte headroom and 2-byte tailroom are part of every allocation and are *not* counted as usable size: the class holds `size + HEADROOM + TAILROOM`, so `slice(0..len)` always has 16 bytes addressable before and 2 after. `slice` returns `SliceError::NoHeadroom` if the requested range would violate that.

`cap_bytes` is charged at allocation (class size), not at `get`. `free_bytes` is `cap - charged`.

- [ ] **Step 4: Run again and commit**

Run: `cargo test --lib chunk::tests`
Expected: PASS. Commit: `feat: pooled chunks with headroom and a byte cap`.

---

### Task 3: The timer heap

**Files:**
- Create: `src/io/timer.rs`, `src/io/mod.rs`
- Test: inline tests in `src/io/timer.rs`

**Interfaces:**
- Consumes: `id::{ConnId, RawId}`.
- Produces: `TimerKind { Idle, ReadProgress, WriteProgress, Drain, HalfClose }`; `TimerHeap::arm(&mut self, ConnId, TimerKind, Instant)`; `TimerHeap::next_deadline(&self) -> Option<Instant>`; `TimerHeap::pop_due(&mut self, now: Instant) -> Vec<TimerEvent>`; `TimerHeap::purge(&mut self, ConnId)`; `TimerEvent { conn: ConnId, kind: TimerKind, arm: u64 }`; `TimerHeap::arm_of(&self, ConnId, TimerKind) -> u64`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn rearm_bumps_arm_counter_so_a_queued_event_is_stale() {
    let mut h = TimerHeap::new();
    let c = ConnId::from(RawId { index: 0, generation: 0 });
    let t0 = Instant::now();
    h.arm(c, TimerKind::Idle, t0);
    let first = h.pop_due(t0).pop().unwrap();
    h.arm(c, TimerKind::Idle, t0 + Duration::from_millis(50));
    assert_ne!(h.arm_of(c, TimerKind::Idle), first.arm); // stale check is possible
}

#[test]
fn purged_timer_never_fires() {
    let mut h = TimerHeap::new();
    let c = ConnId::from(RawId { index: 1, generation: 3 });
    h.arm(c, TimerKind::Drain, Instant::now());
    h.purge(c);
    assert!(h.pop_due(Instant::now() + Duration::from_secs(1)).is_empty());
}

#[test]
fn next_deadline_is_the_earliest() {
    let mut h = TimerHeap::new();
    let now = Instant::now();
    let a = ConnId::from(RawId { index: 0, generation: 0 });
    let b = ConnId::from(RawId { index: 1, generation: 0 });
    h.arm(a, TimerKind::Idle, now + Duration::from_secs(5));
    h.arm(b, TimerKind::Idle, now + Duration::from_secs(2));
    assert_eq!(h.next_deadline(), Some(now + Duration::from_secs(2)));
}
```

- [ ] **Step 2: Run them** — `cargo test --lib io::timer::tests` — Expected: FAIL.

- [ ] **Step 3: Implement**

A `BinaryHeap<Reverse<Entry>>` keyed by deadline, with a `HashMap<(ConnId, TimerKind), Entry>` for O(1) re-arm/purge and a per-entry `arm: u64` counter bumped on every arm. `pop_due` pops entries whose deadline ≤ now and returns them with their arm counters; entries marked cancelled are dropped. `purge` removes every key for that connection and marks them cancelled.

- [ ] **Step 4: Run again and commit** — `feat: single-owner timer heap with stale-event detection`.

---

### Task 4: `Backend` trait, `Socket`, and the epoll backend

**Files:**
- Create: `src/io/backend.rs`, `src/io/socket.rs`, `src/io/epoll.rs`
- Test: inline tests in `src/io/epoll.rs` using `std::os::unix::net::UnixStream::pair()`

**Interfaces:**
- Consumes: `io::Interest`.
- Produces: `socket::Socket` with `bind(addr)`, `local_addr()`, `connect(addr)`, `accept()`, `recv(&mut [u8]) -> io::Result<usize>`, `send(&[u8]) -> io::Result<usize>`, `shutdown_write()`, `set_nonblocking()`, `close()`; `backend::RawEvent { key: u64, readable: bool, writable: bool, error: bool, hangup: bool }`; `trait Backend: private::Sealed { fn register(&mut self, sock: &Socket, interest: Interest, key: u64) -> io::Result<()>; fn reregister(&mut self, sock: &Socket, interest: Interest, key: u64) -> io::Result<()>; fn deregister(&mut self, sock: &Socket) -> io::Result<()>; fn wait(&mut self, timeout: Option<Duration>) -> io::Result<()>; fn next(&mut self) -> Option<RawEvent>; fn wake(&mut self) -> io::Result<()>; }`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn readiness_is_level_triggered_until_drained() {
    let (mut a, mut b) = UnixStream::pair().unwrap();
    a.set_nonblocking(true).unwrap();
    let mut be = EpollBackend::new().unwrap();
    be.register(&Socket::from_fd(a.as_raw_fd()), Interest::READ, 7).unwrap();
    b.write_all(b"x").unwrap();
    be.wait(Some(Duration::from_millis(100))).unwrap();
    let ev = be.next().unwrap();
    assert_eq!(ev.key, 7);
    assert!(ev.readable);
    // do NOT drain; wait again: the same fd must be reported again
    be.wait(Some(Duration::from_millis(100))).unwrap();
    assert_eq!(be.next().unwrap().key, 7);
}
```

- [ ] **Step 2: Run it** — `cargo test --lib io::epoll::tests` — Expected: FAIL.

- [ ] **Step 3: Implement**

`epoll_create1(EPOLL_CLOEXEC)`, `epoll_ctl` only when the interest mask actually changes (track the last mask per key), `epoll_wait` into a small fixed buffer, one `RawEvent` per returned entry. Level-triggered: never set `EPOLLET`. `wake()` writes one byte to an internal `eventfd`.

`Socket` is `Copy`-ish over `RawFd` (owned by the driver); every call returns `io::Error` and never retries.

- [ ] **Step 4: Run again and commit** — `feat: epoll backend, level-triggered readiness`.

---

### Task 5: Driver table, `listen`/`accept`, and event ordering

**Files:**
- Create: `src/io/driver.rs`
- Modify: `src/lib.rs` (export `Driver`, `Config`)
- Test: `tests/transport.rs`

**Interfaces:**
- Consumes: `IdTable`, `ChunkPool`, `Backend`, `Socket`, `TimerHeap`.
- Produces:
```rust
impl Driver {
    pub fn new(config: Config) -> Result<Driver, Error>;
    pub fn listen(&mut self, addr: SocketAddr) -> Result<ListenerId, Error>;
    pub fn local_addr(&self, l: ListenerId) -> Result<SocketAddr, Error>;
    pub fn accept(&mut self, l: ListenerId) -> Result<(ConnId, SocketAddr), Error>;
    pub fn activate(&mut self, c: ConnId) -> Result<(), Error>;   // starts read interest
    pub fn wait(&mut self) -> Result<(), Error>;
    pub fn next_io(&mut self) -> Option<Io>;
    pub fn active(&self) -> impl Iterator<Item = ConnId> + '_;
    pub fn close(&mut self, c: ConnId);
}
pub struct Config { pub pool: PoolConfig, pub batch_budget: usize }
```

- [ ] **Step 1: Write the failing tests** (with the shared helpers every later test uses)

```rust
// tests/transport.rs
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
                let (c, _) = d.accept(l).unwrap();
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
fn accepted_socket_has_no_read_interest_until_activate() {
    let mut d = driver();
    let (l, addr) = listening(&mut d);
    let mut peer = std::net::TcpStream::connect(addr).unwrap();
    peer.write_all(b"early").unwrap();

    d.wait().unwrap();
    let c = loop {
        let mut found = None;
        while let Some(io) = d.next_io() {
            if matches!(io, Io::Acceptable(_)) { found = Some(d.accept(l).unwrap().0); }
        }
        if let Some(c) = found { break c; }
        d.wait().unwrap();
    };
    // no Readable before activate: the id must not appear in the app before it knows it
    d.wait().unwrap();
    while let Some(io) = d.next_io() {
        assert!(!matches!(io, Io::Readable(id) if id == c));
    }

    d.activate(c).unwrap();
    let mut chunk = d.pool().get(64).unwrap();
    assert!(matches!(d.recv(c, &mut chunk).unwrap(), Read::Data(5)));
}
```

- [ ] **Step 2: Run them** — `cargo test --test transport` — Expected: FAIL.

- [ ] **Step 3: Implement**

`Config::default()`: pool cap 64 MiB, classes `&[4096, 17_432, 66_560]`, `batch_budget: 512`. `Driver` holds `IdTable<Conn>`, `IdTable<Listener>`, the backend, the timer heap, a `Vec<Io>` event queue, a live-id set, and `active: Vec<ConnId>`.

`accept` creates the socket and inserts it **without read interest**; `activate` turns it on. `close` marks the id dead, purges timers, deregisters from the backend, and pushes `Io::Closed(id, reason)` at the **tail** of the queue. `next_io` filters every queued event against the live set before yielding, which is what makes `Closed` last even when the batch already contained a `Writable` for the same id.

- [ ] **Step 4: Run again and commit** — `feat: driver table, listen/accept/close with ordered events`.

---

### Task 6: `connect` and spurious-readiness tolerance

**Files:**
- Modify: `src/io/driver.rs`
- Test: `tests/transport.rs`

**Interfaces:**
- Consumes: everything from Task 5.
- Produces: `Driver::connect(&mut self, addr: SocketAddr) -> Result<ConnId, Error>`; `Io::Connected(ConnId)`, `Io::ConnectFailed(ConnId, ConnectError)`; `Driver::recv(&mut self, c: ConnId, buf: &mut ChunkMut) -> Result<Read, Error>` with `Read { Data(usize), WouldBlock, Eof }`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn connect_success_emits_connected_exactly_once() {
    let peer = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut d = driver();
    let c = d.connect(peer.local_addr().unwrap()).unwrap();

    let mut n = 0;
    for _ in 0..64 {
        d.wait().unwrap();
        while let Some(io) = d.next_io() {
            if matches!(io, Io::Connected(id) if id == c) { n += 1; }
        }
        if n > 0 { break; }
    }
    assert_eq!(n, 1);
}

#[test]
fn connect_refused_emits_connect_failed() {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);                                   // nothing is listening on that port now
    let mut d = driver();
    let c = d.connect(addr).unwrap();

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
fn recv_returns_would_block_not_an_error_when_the_event_was_spurious() {
    let mut d = driver();
    let (l, addr) = listening(&mut d);
    let mut peer = std::net::TcpStream::connect(addr).unwrap();
    peer.write_all(b"hello").unwrap();
    let c = accept_one(&mut d, l);

    let mut chunk = d.pool().get(64).unwrap();
    assert!(matches!(d.recv(c, &mut chunk).unwrap(), Read::Data(5)));
    // the readiness that produced the first read is still pending; the second call must not be an error
    assert!(matches!(d.recv(c, &mut chunk).unwrap(), Read::WouldBlock));
}
```

- [ ] **Step 2: Run them** — Expected: FAIL.

- [ ] **Step 3: Implement**

`connect` creates a non-blocking socket, calls `connect`, and registers write interest; `EINPROGRESS` is normal. On `Writable`, `take_error()` decides `Connected` vs `ConnectFailed`, then interest drops back to READ. `recv` calls `recv(2)` once; `0` becomes `Read::Eof` (the driver latches `read_shut` and reports `Readable` once more so the app can call `feed_eof`), `-1` with `WouldBlock` becomes `Read::WouldBlock`, anything else is `Error::Io`.

- [ ] **Step 4: Run again and commit** — `feat: non-blocking connect and EOF latch`.

---

### Task 7: `send`, interest management and partial writes

**Files:**
- Modify: `src/io/driver.rs`, `src/io/mod.rs`
- Test: `tests/transport.rs`

**Interfaces:**
- Consumes: Task 6.
- Produces: `Driver::send(&mut self, c: ConnId, buf: &[u8]) -> Result<Sent, Error>` with `Sent { All, Partial(usize), WouldBlock }`; `Driver::interest(&mut self, c: ConnId, i: Interest) -> Result<(), Error>`; `Io::Writable(ConnId)`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn big_send_is_partial_and_never_claims_all() {
    let mut d = driver();
    let (l, addr) = listening(&mut d);
    let _peer = std::net::TcpStream::connect(addr).unwrap();   // never reads
    let c = accept_one(&mut d, l);
    let big = vec![0u8; 8 << 20];

    match d.send(c, &big).unwrap() {
        Sent::Partial(n) => assert!(n > 0 && n < big.len()),
        Sent::WouldBlock => {}
        Sent::All => panic!("8 MiB cannot fit a default socket buffer"),
    }
}

#[test]
fn send_on_a_full_buffer_is_would_block_not_an_error() {
    let mut d = driver();
    let (l, addr) = listening(&mut d);
    let _peer = std::net::TcpStream::connect(addr).unwrap();   // never reads
    let c = accept_one(&mut d, l);
    let big = vec![0u8; 1 << 20];

    let mut would_block = false;
    for _ in 0..64 {
        match d.send(c, &big).unwrap() {          // `unwrap`: WouldBlock must not be an Err
            Sent::All | Sent::Partial(_) => {}
            Sent::WouldBlock => { would_block = true; break; }
        }
    }
    assert!(would_block);
}

#[test]
fn interest_flip_is_skipped_when_the_mask_is_unchanged() {
    // the backend keeps `ctl_calls: usize` under cfg(test), exposed as backend_ctl_calls()
    let mut d = driver();
    let (l, addr) = listening(&mut d);
    let _peer = std::net::TcpStream::connect(addr).unwrap();
    let c = accept_one(&mut d, l);

    d.interest(c, Interest::READ | Interest::WRITE).unwrap();
    let before = d.backend_ctl_calls();
    d.interest(c, Interest::READ | Interest::WRITE).unwrap();
    assert_eq!(d.backend_ctl_calls(), before);
}
```

- [ ] **Step 2: Run them** — Expected: FAIL.

- [ ] **Step 3: Implement**

`send` uses `send(2)` with `MSG_NOSIGNAL`; `WouldBlock` maps to `Sent::WouldBlock`; short writes map to `Sent::Partial(n)` and the driver turns write interest on; when the app drains, it turns it off. Interest changes are compared against the last registered mask and skipped when equal. `EPIPE`/`ECONNRESET` produce `Error::Io` and the driver schedules `Closed(.., PeerReset)`.

- [ ] **Step 4: Run again and commit** — `feat: partial sends and interest flips`.

---

### Task 8: Pausing reads when the pool is exhausted

**Files:**
- Modify: `src/io/driver.rs`, `src/chunk.rs`
- Test: `tests/transport.rs`

**Interfaces:**
- Consumes: Task 7, `ChunkPool::free_bytes`.
- Produces: `Driver::pause_reads(&mut self, c: ConnId)`, `Driver::paused(&self) -> usize`, and the watermark check inside `wait()`.

- [ ] **Step 1: Write the failing tests**

```rust
// fills a 4 KiB pool, then produces a connection that the app has paused
fn paused_setup() -> (Driver, ConnId, Vec<ChunkMut>) {
    let mut d = Driver::new(Config {
        pool: PoolConfig { cap_bytes: 4096, classes: &[2048] },
        ..Default::default()
    }).unwrap();
    let (l, addr) = listening(&mut d);
    let mut peer = std::net::TcpStream::connect(addr).unwrap();
    let c = accept_one(&mut d, l);
    let held: Vec<_> = (0..2).map(|_| d.pool().get(2048).unwrap()).collect();
    peer.write_all(&[0u8; 8192]).unwrap();
    peer.set_nonblocking(true).unwrap();
    d.wait().unwrap();
    while let Some(io) = d.next_io() {
        if matches!(io, Io::Readable(id) if id == c) {
            assert_eq!(d.pool().get(2048), Err(PoolExhausted));
            d.pause_reads(c);
        }
    }
    assert_eq!(d.paused(), 1);
    (d, c, held)
}

#[test]
fn exhausted_pool_pauses_reads_and_does_not_spin() {
    let (mut d, c, _held) = paused_setup();
    let t = Instant::now();
    d.wait().unwrap();                          // must not report c again, and must not spin
    assert!(t.elapsed() < Duration::from_millis(50));
    while let Some(io) = d.next_io() {
        assert!(!matches!(io, Io::Readable(id) if id == c));
    }
}

#[test]
fn returning_chunks_resumes_reads() {
    let (mut d, c, held) = paused_setup();
    drop(held);                                 // pool crosses the watermark
    let mut readable = false;
    for _ in 0..8 {
        d.wait().unwrap();
        while let Some(io) = d.next_io() {
            if matches!(io, Io::Readable(id) if id == c) { readable = true; }
        }
        if readable { break; }
    }
    assert!(readable);
}
```

- [ ] **Step 2: Run them** — Expected: FAIL.

- [ ] **Step 3: Implement**

`pause_reads` clears `Interest::READ` and records the id in a `paused` list. `wait()` first checks whether `pool.free_bytes()` is above the watermark (25% of cap); if so it clears read interest for the paused ids and re-arms them, in one batch. A pause is never silent: `paused()` is observable in tests and metrics.

- [ ] **Step 4: Run again and commit** — `feat: pause reads on pool exhaustion`.

---

### Task 9: `close`, half-close and graceful shutdown

**Files:**
- Modify: `src/io/driver.rs`
- Test: `tests/transport.rs`

**Interfaces:**
- Consumes: Tasks 5–8.
- Produces: `Driver::close(&mut self, c: ConnId)`, `Driver::shutdown_write(&mut self, c: ConnId) -> Result<(), Error>`, `Io::HalfClose(ConnId)`, `CloseReason { Local, Peer, PeerReset, TimedOut(TimerKind), Io(i32) }`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn half_close_keeps_the_read_side_alive() {
    let mut d = driver();
    let (l, addr) = listening(&mut d);
    let mut peer = std::net::TcpStream::connect(addr).unwrap();
    let c = accept_one(&mut d, l);

    d.shutdown_write(c).unwrap();
    let mut buf = [0u8; 16];
    assert_eq!(peer.read(&mut buf).unwrap(), 0);      // peer sees EOF on its read side
    peer.write_all(b"still here").unwrap();           // and can still send
    let mut chunk = d.pool().get(64).unwrap();
    assert!(matches!(d.recv(c, &mut chunk).unwrap(), Read::Data(10)));
}

#[test]
fn close_is_idempotent_and_delivers_one_closed() {
    let mut d = driver();
    let (l, addr) = listening(&mut d);
    let _peer = std::net::TcpStream::connect(addr).unwrap();
    let c = accept_one(&mut d, l);

    d.close(c);
    d.close(c);                                       // no panic, no second event
    let mut n = 0;
    for _ in 0..2 {
        d.wait().unwrap();
        while let Some(io) = d.next_io() {
            if matches!(io, Io::Closed(id, _) if id == c) { n += 1; }
        }
    }
    assert_eq!(n, 1);
}

#[test]
fn closed_is_terminal_even_when_a_writable_was_queued_first() {
    let mut d = driver();
    let (l, addr) = listening(&mut d);
    let _peer = std::net::TcpStream::connect(addr).unwrap();
    let c = accept_one(&mut d, l);
    d.interest(c, Interest::READ | Interest::WRITE).unwrap();

    d.wait().unwrap();
    let mut events = Vec::new();
    while let Some(io) = d.next_io() {
        events.push(io.clone());
        if matches!(io, Io::Writable(id) if id == c) { d.close(c); }   // close mid-batch
    }
    if let Some(p) = events.iter().position(|e| matches!(e, Io::Closed(id, _) if *id == c)) {
        assert!(events[p + 1..].iter().all(|e| !matches!(e, Io::Writable(id) if *id == c)));
    }
}
```

- [ ] **Step 2: Run them** — Expected: FAIL.

- [ ] **Step 3: Implement**

`shutdown_write` calls `shutdown(fd, SHUT_WR)` once and records it; a second call is a no-op. A read of `0` after a local `shutdown_write` still yields `HalfClose`, not `Peer`. `close` is idempotent: a second call is a no-op, not an error. `CloseReason` is chosen from the syscall error, the shutdown flags and the latch state.

- [ ] **Step 4: Run again and commit** — `feat: half-close and idempotent close`.

---

### Task 10: kqueue backend

**Files:**
- Create: `src/io/kqueue.rs`
- Modify: `src/io/mod.rs` (cfg wiring), `.github/workflows/ci.yml`
- Test: `tests/transport.rs` (same suite; runs on macOS)

**Interfaces:**
- Consumes: `Backend`, `Socket`, `Interest`.
- Produces: `KqueueBackend` implementing `Backend`.

- [ ] **Step 1: Make the suite fail on macOS**

Run the Task 5–9 tests on macOS, with `src/io/mod.rs` selecting `kqueue.rs` under `cfg(any(target_os = "macos", target_os = "freebsd"))`.
Expected: FAIL — `KqueueBackend` not found.

- [ ] **Step 2: Implement**

`kqueue()`; one `EVFILT_READ` and one `EVFILT_WRITE` kevent per interested socket, `udata` carrying the key; `EV_ADD | EV_ENABLE` and `EV_DELETE` for deregistration; **no `EV_CLEAR`** — level-triggered is the default and this plan depends on it. `wake()` uses `EVFILT_USER`.

- [ ] **Step 3: Run and commit** — `cargo test --test transport` on macOS — Expected: PASS — `feat: kqueue backend`.

---

### Task 11: Windows AFD backend, self-test and fallback

**Files:**
- Create: `src/io/windows.rs`
- Modify: `src/io/mod.rs`, `.github/workflows/ci.yml`
- Test: `tests/transport.rs` (runs on Windows), inline `#[cfg(test)]` self-test

**Interfaces:**
- Consumes: `Backend`, `Socket`, `Interest`.
- Produces: `AfdBackend` implementing `Backend`; `fn afd_self_test() -> Result<(), AfdUnavailable>`; `enum WindowsPath { Afd, WsaPoll }` exposed in `Driver::backend_path()`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn afd_reports_again_when_an_event_was_not_drained() {
    // socketpair; arm AFD poll; write 1 byte; wait for AFD_POLL_RECEIVE;
    // do NOT read; re-arm; assert the event is reported again (level-triggered)
}
```

- [ ] **Step 2: Run it** — `cargo test --test transport afd -- --nocapture` — Expected: FAIL.

- [ ] **Step 3: Implement**

Open `\Device\Afd`, `NtDeviceIoControlFile` with `IOCTL_AFD_POLL` per socket, completions delivered through the IOCP that the same `Backend` drains with `GetQueuedCompletionStatusEx`. The poll is re-issued after each completion (the API-level re-arm claim in the spec is about the *application*, not this layer).

`afd_self_test()` runs a socketpair through read, write, abort, and the not-drained re-report case above. If it fails, `WindowsPath::WsaPoll` is selected: `WSAPoll` for read/write readiness plus `ConnectEx` completions for connect. Every `Driver` exposes which path it chose.

- [ ] **Step 4: Run and commit** — `feat: Windows AFD backend with self-test and WSAPoll fallback`.

---

### Task 12: Datagram sockets

**Files:**
- Modify: `src/io/driver.rs`, `src/io/socket.rs`, `src/lib.rs`
- Test: `tests/transport.rs`

**Interfaces:**
- Consumes: Tasks 5–7.
- Produces: `Driver::bind_udp(&mut self, addr: SocketAddr) -> Result<UdpId, Error>`; `Driver::udp_local_addr(&self, u: UdpId) -> Result<SocketAddr, Error>`; `Driver::recv_datagrams(&mut self, u: UdpId, out: &mut [Datagram<'_>]) -> Result<usize, Error>`; `Driver::send_datagrams(&mut self, u: UdpId, out: &[OutDatagram<'_>]) -> Result<usize, Error>`; `pub struct Datagram<'a> { pub bytes: &'a mut [u8], pub len: usize, pub peer: SocketAddr, pub ecn: Option<Ecn> }`; `pub struct OutDatagram<'a> { pub bytes: &'a [u8], pub peer: SocketAddr, pub ecn: Option<Ecn> }`; `pub struct Ecn(pub u8)`; `Io::Datagram(UdpId)`.

- [ ] **Step 1: Write the failing test**

```rust
// waits until the driver reports a datagram on `u` (or panics after 64 waits)
fn wait_for_datagram(d: &mut Driver, u: UdpId) { /* loop wait + next_io, assert Io::Datagram(u) */ }

#[test]
fn datagrams_round_trip_with_peer_addresses() {
    let mut d = driver();
    let a = d.bind_udp("127.0.0.1:0".parse().unwrap()).unwrap();
    let b = d.bind_udp("127.0.0.1:0".parse().unwrap()).unwrap();
    let (pa, pb) = (d.udp_local_addr(a).unwrap(), d.udp_local_addr(b).unwrap());

    let sent = d.send_datagrams(a, &[
        OutDatagram { bytes: b"one", peer: pb, ecn: None },
        OutDatagram { bytes: b"two", peer: pb, ecn: None },
    ]).unwrap();
    assert_eq!(sent, 2);

    wait_for_datagram(&mut d, b);
    let mut b1 = [0u8; 64];
    let mut b2 = [0u8; 64];
    let mut out = [
        Datagram { bytes: &mut b1, len: 0, peer: pa, ecn: None },
        Datagram { bytes: &mut b2, len: 0, peer: pa, ecn: None },
    ];
    let n = d.recv_datagrams(b, &mut out).unwrap();
    assert_eq!(n, 2);
    assert_eq!(&out[0].bytes[..out[0].len], b"one");
    assert_eq!(out[0].peer, pa);
}
```

- [ ] **Step 2: Run it** — `cargo test --test transport datagrams` — Expected: FAIL.

- [ ] **Step 3: Implement**

`bind_udp` creates a non-blocking UDP socket and registers read interest, producing `Io::Datagram` (not `Readable`) on arrival. `recv_datagrams` drains with `recvmmsg` where available and a `recvfrom` loop otherwise, filling `len`, `peer` and `ecn` (from `IP_RECVTOS`/`IPV6_RECVTCLASS`; `None` when the platform does not provide it — never fabricated). `send_datagrams` uses `sendmmsg` where available and a `sendto` loop otherwise, returning how many were accepted. `recv_datagrams` returns `0` on `WouldBlock` rather than an error, matching `Read`.

- [ ] **Step 4: Run again and commit** — `feat: datagram sockets with batched send and receive`.

---

## Self-review notes (kept so the next plan inherits them)

- The credit handle from the spec (§7) is **not** in this plan on purpose: h1 has no flow-control window, so nothing in M1 consumes credit. It belongs to the HTTP/2 plan, and the transport already exposes the hook (`free_bytes`, per-connection state) it needs.
- `Drained` is a connection-object event, not an `Io` event: the driver cannot know whether the application still holds the tail of a partial send. The spec was corrected to match.
- The next plans in this sequence are: h1 engine + `Conn`, then the h1↔h1 proxy and Gate 1.
