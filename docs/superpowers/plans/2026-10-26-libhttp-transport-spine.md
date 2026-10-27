# Transport Spine (M1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the platform transport spine — blocks/pools, timers, wakeups, socket slab, native pollers (epoll, kqueue, IOCP), accept/connect/read/write/close — proven by a byte-echo server on Linux, macOS, FreeBSD and Windows, with the 10k-idle-connection budget measured.

**Architecture:** A crate-internal reactor owns a socket slab, a block pool and a timer heap. Platform backends are native: epoll/kqueue use edge-triggered readiness and perform the syscall on the event; IOCP posts one overlapped receive/send per connection and delivers completions. A shared `Handler` trait receives accepted connections and plaintext byte chunks; M1's only implementation echoes bytes. HTTP engines and the public API arrive in later plans.

**Tech Stack:** Rust edition 2024, MSRV 1.85.0, `libc` on Unix, `windows-sys` on Windows, no async runtime, no `bytes`-style buffer, no third-party event loop.

**Spec:** [top-level design](../specs/2026-10-26-libhttp-sync-http-design.md), [runtime design](../specs/2026-10-26-libhttp-runtime-design.md), [execution design](../specs/2026-10-26-libhttp-execution-design.md).

This is plan 1 of 6. M2 (shared Runtime + HTTP/1.1 + workers + typed headers + deferred completion + the four client verbs `send`/`send_with`/`fetch`/`fetch_with`), M3 (TLS), M4 (HTTP/2), M5 (HTTP/3) and M6 (hardening) get separate plans because each produces a working, independently testable increment.

## Global Constraints

- Rust edition 2024; `rust-version = "1.85"`; no nightly-only features.
- No `Future`, `Waker`, executor, async runtime, `tokio`, `hyper`, `h2`, `mio`, or `socket2` in the product.
- No blocking socket I/O on reactor threads; every socket is nonblocking; Windows uses overlapped I/O + IOCP.
- No thread per connection.
- All queues and buffers are bounded; every bound has a reaction.
- Blocks are 16 KiB. `Block` is `Send + Sync`, refcounted, and returns to its origin pool on last drop.
- One logical read and one write in flight per connection; pause by clearing interest / not reposting; close cancels and waits where required.
- Timers: one heap per reactor; entries are `(TimerKey{slot,generation,exchange_seq}, timer_generation, deadline, kind)`; stale key/generation entries are skipped.
- Unsafe code is confined to `src/rt/sys/` and the block refcount internals; every block is documented and wrapped safely. The crate builds with `#![forbid(unsafe_op_in_unsafe_fn)]`.
- Platform targets: Linux, macOS, FreeBSD, Windows; CI runs functional tests on all four (FreeBSD in a VM).
- M1 adds no public API and no HTTP; `pub(crate)` only until M2.
- Every commit leaves `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test --all-targets` green on the host.

## Review Focus

These are the failure modes M1's tests must pin, most likely first:

1. **Partial and spurious readiness.** A read/write returns fewer bytes or `EAGAIN` after an event; the reactor must not lose data, spin, or treat `EAGAIN` as EOF. Tests: `read_partial_then_eagain`, `write_partial_then_writable`, `spurious_read_event_is_harmless`.
2. **Close with an in-flight operation and slot reuse.** A connection closes while a read/write/overlapped operation is outstanding; late completion or a reused slot must not affect the next connection. Tests: `close_with_pending_read`, `stale_completion_after_reuse_is_ignored` (Windows).
3. **Stale timers after slot reuse.** A timeout from connection N must not fire against N+1 on an h1 keep-alive slot. Tests: `stale_timer_generation_is_skipped`, `timer_reinsert_cancels_previous`.
4. **Lost or uncoalesced wakeups.** A cross-thread wake between poll iterations must not be lost; many wakes must drain once. Tests: `wake_before_poll_is_seen`, `many_wakes_coalesce`.
5. **Accept/connect addressing.** Accepted and connected sockets preserve peer/local addresses for IPv4 and IPv6; a failed connect reports a typed error without leaking the socket. Tests: `accept_ipv4_ipv6`, `connect_refused_closes_socket`.

---

## File Structure

```
Cargo.toml                         # edition, rust-version, libc/windows-sys, no product deps
src/lib.rs                        # crate docs, #![forbid], module declarations (no public API yet)
src/rt/mod.rs                     # internal exports: Block, BlockPool, TimerHeap, Conn, Handler, Reactor
src/rt/block.rs                   # refcounted 16 KiB block + ranges + per-origin pool
src/rt/pool.rs                    # bounded block pool + cross-thread return queue
src/rt/timer.rs                   # generation-checked timer heap
src/rt/wake.rs                    # Unix eventfd/pipe wakeup; Windows wake is in iocp.rs
src/rt/socket.rs                  # OwnedSocket wrapper, nonblocking, shutdown, addresses
src/rt/slab.rs                    # ConnId slab with generations
src/rt/conn.rs                    # Conn state, bounded write queue, pause/resume
src/rt/handler.rs                 # Handler trait + EchoHandler
src/rt/reactor/mod.rs             # shared reactor loop skeleton, Conn lifecycle
src/rt/reactor/unix.rs            # readiness reactor over Poller (epoll/kqueue)
src/rt/poller/mod.rs              # Poller trait, Interest, PollEvent
src/rt/poller/epoll.rs            # Linux epoll backend (EPOLLET, EPOLL_CTL_MOD)
src/rt/poller/kqueue.rs           # macOS/FreeBSD kqueue backend (EV_CLEAR)
src/rt/reactor/iocp.rs            # Windows IOCP reactor (AcceptEx/ConnectEx/WSARecv/WSASend)
src/rt/sys/mod.rs                 # unsafe syscall shims only
src/rt/test_support.rs            # cfg(test): loopback harness, counting allocator helpers
tests/                            # none; M1 tests are crate-internal (no public API yet)
.github/workflows/ci.yml          # matrix: ubuntu, macos, windows, freebsd VM; MSRV job
```

---

### Task 1: Crate skeleton, internal module tree, CI gate

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/lib.rs`
- Create: `src/rt/mod.rs`, `src/rt/sys/mod.rs`
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Produces: `libhttp::rt` (crate-private) module tree; MSRV/feature baseline for all later tasks.

- [ ] **Step 1: Write the failing smoke test**

In `src/lib.rs`:

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn crate_identity_is_libhttp() {
        assert_eq!(env!("CARGO_PKG_NAME"), "libhttp");
        assert_eq!(env!("CARGO_PKG_VERSION"), "0.1.0");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib crate_identity_is_libhttp`
Expected: PASS only after the manifest sets the MSRV/edition; this is the setup/smoke step (no product code yet).

- [ ] **Step 3: Set manifest and module tree**

`Cargo.toml`:

```toml
[package]
name = "libhttp"
version = "0.1.0"
edition = "2024"
rust-version = "1.85"

[dependencies]
[target.'cfg(unix)'.dependencies]
libc = "0.2"
[target.'cfg(windows)'.dependencies]
windows-sys = { version = "0.61", features = [
    "Win32_Foundation",
    "Win32_Networking_WinSock",
    "Win32_System_IO",
    "Win32_System_Threading",
] }
```

`src/lib.rs` starts with `#![forbid(unsafe_op_in_unsafe_fn)]`, declares `mod rt;`, and the test above. Create an empty file for every module declared in File Structure and list it in `src/rt/mod.rs`; do not declare a module whose file does not exist. `src/rt/sys/mod.rs` is empty.

- [ ] **Step 4: Run test and checks**

Run: `cargo test --lib crate_identity_is_libhttp && cargo metadata --no-deps --format-version 1 | grep -q '"rust_version":"1.85"' && cargo fmt --check && cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml src/lib.rs src/rt .github/workflows/ci.yml
git commit -m "chore: crate skeleton, MSRV and CI matrix"
```

The CI file: a matrix job on `ubuntu-latest`, `macos-latest`, `windows-latest` and a `vmactions/freebsd-vm` job, each running `cargo test --all-targets`; a separate `msrv` job installs 1.85.0 and runs `cargo test --all-targets`; `fmt`/`clippy` on stable.

---

### Task 2: Refcounted 16 KiB blocks and bounded pool

**Files:**
- Create: `src/rt/block.rs`, `src/rt/pool.rs`
- Modify: `src/rt/mod.rs`

**Interfaces:**
- Produces:
  - `pub(crate) const BLOCK_SIZE: usize = 16 * 1024;`
  - `pub(crate) struct Block` (Send + Sync, refcounted, range into a 16 KiB allocation)
  - `Block::alloc(&Arc<BlockPool>) -> Option<Block>`
  - `Block::len(&self) -> usize`, `as_slice(&self) -> &[u8]`, `as_mut_slice(&mut self) -> &mut [u8]`
  - `Block::split_at(self, mid: usize) -> (Block, Block)` (both ranges share the allocation; panics only on `mid > len`)
  - `BlockPool::new(cap_bytes: usize) -> Arc<BlockPool>`, `alloc(self: &Arc<Self>) -> Option<Block>`, `free_blocks(&self) -> usize`, `cap_blocks(&self) -> usize`

- [ ] **Step 1: Write the failing tests**

In `src/rt/block.rs` `#[cfg(test)] mod tests`:

```rust
#[test]
fn split_shares_one_allocation() {
    let pool = BlockPool::new(BLOCK_SIZE * 4);
    let mut block = pool.alloc().unwrap();
    block.as_mut_slice()[..6].copy_from_slice(b"abcdef");
    let (left, right) = block.split_at(3);
    assert_eq!(left.as_slice(), b"abc");
    assert_eq!(right.as_slice(), b"def");
    drop(left);
    assert_eq!(right.as_slice(), b"def");
}

#[test]
fn pool_is_bounded_and_reuses_blocks() {
    let pool = BlockPool::new(BLOCK_SIZE * 2);
    let a = pool.alloc().unwrap();
    let _b = pool.alloc().unwrap();
    assert!(pool.alloc().is_none());
    drop(a);
    assert_eq!(pool.free_blocks(), 1);
    let _c = pool.alloc().unwrap();
}

#[test]
fn block_returns_to_origin_pool_from_another_thread() {
    let pool = BlockPool::new(BLOCK_SIZE);
    let block = pool.alloc().unwrap();
    std::thread::spawn(move || drop(block)).join().unwrap();
    assert_eq!(pool.free_blocks(), 1);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib rt::block`
Expected: FAIL with unresolved `BlockPool`/`Block`.

- [ ] **Step 3: Implement `Block`/`BlockPool`**

Use `Arc<BlockInner>` with an atomic refcount; `Block` holds the `Arc` plus `start/end`; `split_at` clones the `Arc` and adjusts ranges. The pool owns a `Mutex<Vec<Arc<BlockInner>>>` (or `Mutex<Vec<*mut ...>>` with safe ownership), a cap in blocks, and an origin queue; `Drop` for the last range returns the allocation to the origin pool if it is still alive, otherwise frees it. `Block` must not expose `unsafe` to callers. Keep the 16 KiB size in one `const`.

- [ ] **Step 4: Run tests and checks**

Run: `cargo test --lib rt::block && cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/rt/block.rs src/rt/pool.rs src/rt/mod.rs
git commit -m "feat(rt): refcounted 16 KiB blocks and bounded pool"
```

---

### Task 3: Generation-checked timer heap

**Files:**
- Create: `src/rt/timer.rs`
- Modify: `src/rt/mod.rs`

**Interfaces:**
- Produces:
  - `pub(crate) struct TimerKey { slot: u32, generation: u32, exchange_seq: u32 }`
  - `pub(crate) struct TimerToken { key: TimerKey, timer_generation: u32 }`
  - `pub(crate) struct TimerHeap`
  - `TimerHeap::new()`
  - `TimerHeap::insert(&mut self, key: TimerKey, deadline: Instant, kind: u16) -> TimerToken`
  - `TimerHeap::cancel(&mut self, token: TimerToken)`
  - `TimerHeap::next_deadline(&self) -> Option<Instant>`
  - `TimerHeap::pop_expired(&mut self, now: Instant) -> Vec<(u16, TimerToken)>`
- `exchange_seq` is carried now because runtime §5 requires it; M1 uses `0` for all echo connections. M2 supplies the real per-exchange sequence.
- The live-key generation is the connection's `ConnId` generation; `timer_generation` distinguishes reinserts for the same key. A stale key or timer generation is skipped.

- [ ] **Step 1: Write the failing tests**

```rust
fn key(slot: u32) -> TimerKey {
    TimerKey { slot, generation: 0, exchange_seq: 0 }
}

#[test]
fn pops_in_deadline_order() {
    let mut h = TimerHeap::new();
    let base = Instant::now();
    let later = h.insert(key(1), base + Duration::from_millis(20), 1);
    let sooner = h.insert(key(2), base + Duration::from_millis(10), 2);
    let got = h.pop_expired(base + Duration::from_millis(30));
    assert_eq!(got, vec![(2, sooner), (1, later)]);
}

#[test]
fn cancel_skips_the_entry() {
    let mut h = TimerHeap::new();
    let t = h.insert(key(7), Instant::now(), 7);
    h.cancel(t);
    assert!(h.pop_expired(Instant::now() + Duration::from_secs(1)).is_empty());
}

#[test]
fn timer_reinsert_cancels_previous() {
    let mut h = TimerHeap::new();
    let old = h.insert(key(7), Instant::now(), 7);
    let new = h.insert(key(7), Instant::now() + Duration::from_secs(10), 7);
    let got = h.pop_expired(Instant::now() + Duration::from_secs(1));
    assert!(got.is_empty());
    let got = h.pop_expired(Instant::now() + Duration::from_secs(11));
    assert_eq!(got, vec![(7, new)]);
    assert_ne!(old.timer_generation, new.timer_generation);
}

#[test]
fn stale_timer_generation_is_skipped() {
    let mut h = TimerHeap::new();
    let t = h.insert(key(7), Instant::now(), 7);
    h.cancel(t);
    let newer = h.insert(key(7), Instant::now(), 7);
    let got = h.pop_expired(Instant::now() + Duration::from_secs(1));
    assert_eq!(got, vec![(7, newer)]);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib rt::timer`
Expected: FAIL with unresolved `TimerHeap`.

- [ ] **Step 3: Implement the heap**

Use `BinaryHeap<Reverse<TimerEntry>>` plus `HashMap<TimerKey, u32>` of the live `timer_generation` per key. `insert` bumps the timer generation for that key and stores it; `cancel` removes the live generation; `pop_expired` skips entries whose key/timer generation is stale and returns `(kind, token)` in deadline order. `next_deadline` peeks and skips stale entries without popping them.

- [ ] **Step 4: Run tests and checks**

Run: `cargo test --lib rt::timer && cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/rt/timer.rs src/rt/mod.rs
git commit -m "feat(rt): generation-checked timer heap"
```

---

### Task 4: Cross-thread reactor wakeup

**Files:**
- Create: `src/rt/wake.rs`
- Modify: `src/rt/mod.rs`

**Interfaces:**
- Produces:
  - `pub(crate) struct Wake`
  - `Wake::new() -> io::Result<Self>`
  - `Wake::wake(&self) -> io::Result<()>`
  - `Wake::drain(&mut self) -> io::Result<bool>` (true if at least one wake was pending)
  - `#[cfg(unix)] Wake::raw_fd(&self) -> RawFd`
  - Windows uses the IOCP sentinel in Task 9; this task compiles only on Unix.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn wake_then_drain() {
    let mut w = Wake::new().unwrap();
    w.wake().unwrap();
    assert!(w.drain().unwrap());
    assert!(!w.drain().unwrap());
}

#[test]
fn many_wakes_coalesce() {
    let mut w = Wake::new().unwrap();
    for _ in 0..64 { w.wake().unwrap(); }
    assert!(w.drain().unwrap());
    assert!(!w.drain().unwrap());
}

#[test]
fn wake_before_poll_is_seen() {
    let mut w = Wake::new().unwrap();
    let fd = w.raw_fd();
    let t = std::thread::spawn(move || unsafe { libc::write(fd, b"x".as_ptr().cast(), 1) });
    t.join().unwrap();
    assert!(w.drain().unwrap());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib rt::wake`
Expected: FAIL with unresolved `Wake`.

- [ ] **Step 3: Implement platform wakeups**

Linux: `eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)`; macOS/FreeBSD: a nonblocking pipe. `wake` writes 8 bytes/1 byte; `drain` reads until `EAGAIN`. Wrap raw fds in an `OwnedFd`-like struct and close on drop. No per-wake allocation.

- [ ] **Step 4: Run tests and checks**

Run: `cargo test --lib rt::wake && cargo clippy --all-targets -- -D warnings`
Expected: PASS on Unix.

- [ ] **Step 5: Commit**

```bash
git add src/rt/wake.rs src/rt/mod.rs
git commit -m "feat(rt): cross-thread reactor wakeup"
```

---

### Task 5: Socket wrapper, generation slab, connection state

**Files:**
- Create: `src/rt/socket.rs`, `src/rt/slab.rs`, `src/rt/conn.rs`
- Modify: `src/rt/mod.rs`

**Interfaces:**
- Produces:
  - `pub(crate) struct OwnedSocket` (nonblocking `TcpStream` or raw socket; `set_nonblocking`, `peer_addr`, `local_addr`, `shutdown_write`, `close`)
  - `pub(crate) struct ConnId { slot: u32, generation: u32 }` (Copy, Eq, Hash)
  - `pub(crate) struct Slab<T>`: `new()`, `insert(T) -> ConnId`, `get(ConnId) -> Option<&T>`, `get_mut`, `remove(ConnId) -> Option<T>`, `len()`
  - `pub(crate) struct Conn`: `new_for_test(write_cap: usize)`, `enqueue(&[u8]) -> Result<(), WriteError>`, `pause_read()`, `resume_read()`, `is_paused()`, `take_pending_write() -> Option<Vec<u8>>`, `close()`

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn slab_generation_prevents_stale_ids() {
    let mut s = Slab::new();
    let id = s.insert(7u32);
    assert_eq!(s.remove(id), Some(7));
    let id2 = s.insert(9u32);
    assert_eq!(id.slot, id2.slot);
    assert_ne!(id.generation, id2.generation);
    assert!(s.get(id).is_none());
    assert_eq!(s.get(id2), Some(&9));
}

#[test]
fn write_queue_is_bounded() {
    let mut c = Conn::new_for_test(16);
    c.enqueue(b"0123456789").unwrap();
    c.enqueue(b"abcdef").unwrap();
    assert!(c.enqueue(b"x").is_err());
    assert!(c.is_paused());
}

#[test]
fn pause_resume_is_visible() {
    let mut c = Conn::new_for_test(64);
    assert!(!c.is_paused());
    c.pause_read();
    assert!(c.is_paused());
    c.resume_read();
    assert!(!c.is_paused());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib rt::slab rt::conn`
Expected: FAIL with unresolved types.

- [ ] **Step 3: Implement the three types**

`Slab<T>` keeps `Vec<Option<Slot<T>>>` and a free list of slot indices; each slot stores a generation bumped on removal. `OwnedSocket` wraps a `std::net::TcpStream` (or raw `SOCKET` on Windows) and is set nonblocking at construction. `Conn` holds the socket id, peer/local addresses, a bounded outbound `VecDeque<Block>` and pause flag; `enqueue` copies into blocks from a pool passed to `Conn::new` (the test constructor uses a tiny cap). This task does not do I/O.

- [ ] **Step 4: Run tests and checks**

Run: `cargo test --lib rt::slab rt::conn && cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/rt/socket.rs src/rt/slab.rs src/rt/conn.rs src/rt/mod.rs
git commit -m "feat(rt): sockets, generation slab and connection state"
```

---

### Task 6: Handler trait, EchoHandler, and the shared reactor loop skeleton

**Files:**
- Create: `src/rt/handler.rs`, `src/rt/reactor/mod.rs`
- Modify: `src/rt/mod.rs`

**Interfaces:**
- Produces:
  - `pub(crate) trait Handler { fn on_accept(&mut self, conn: &mut Conn) -> io::Result<()>; fn on_connect(&mut self, conn: &mut Conn) -> io::Result<()> { Ok(()) } fn on_read(&mut self, conn: &mut Conn, data: &[u8]) -> io::Result<()>; fn on_close(&mut self, id: ConnId); }`
  - `pub(crate) struct EchoHandler` implementing it
  - `ReactorCore::new(handler)`, `ReactorCore::on_read_ready(conn, data)`, `ReactorCore::on_write_ready(conn)` used by both platform reactors
  - Echo behavior: `on_read` enqueues the same bytes; `on_accept` does nothing; `on_close` drops state.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn echo_handler_writes_back_exactly_the_input() {
    let mut h = EchoHandler;
    let mut c = Conn::new_for_test(64);
    h.on_read(&mut c, b"hello").unwrap();
    assert_eq!(c.take_pending_write(), Some(b"hello".to_vec()));
}

#[test]
fn echo_handler_pauses_when_queue_full() {
    let mut h = EchoHandler;
    let mut c = Conn::new_for_test(4);
    h.on_read(&mut c, b"hello").unwrap();
    assert!(c.is_paused());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib rt::handler`
Expected: FAIL with unresolved `EchoHandler`.

- [ ] **Step 3: Implement the handler and core loop**

`EchoHandler::on_read` enqueues `data`; if `enqueue` fails because the queue is full, it leaves the conn paused and the reactor will resume after flushing. `ReactorCore` owns the slab, block pool, timer heap and handler; its `on_read_ready`/`on_write_ready` helpers encode the shared close/error/pause rules. Platform reactors own the OS loop and call these helpers. Do not add HTTP.

- [ ] **Step 4: Run tests and checks**

Run: `cargo test --lib rt::handler && cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/rt/handler.rs src/rt/reactor/mod.rs src/rt/mod.rs
git commit -m "feat(rt): handler seam and shared reactor core"
```

---

### Task 7: Linux epoll backend and loopback echo

**Files:**
- Create: `src/rt/poller/mod.rs`, `src/rt/poller/epoll.rs`, `src/rt/reactor/unix.rs`, `src/rt/test_support.rs`
- Modify: `src/rt/mod.rs`

**Interfaces:**
- Produces:
  - `pub(crate) enum Interest { Read, Write, Both }`
  - `pub(crate) struct PollEvent { pub id: ConnId, pub readable: bool, pub writable: bool, pub error: Option<io::Error> }`
  - `pub(crate) trait Poller { fn add(&mut self, fd: RawFd, id: ConnId, interest: Interest) -> io::Result<()>; fn modify(...); fn remove(...); fn wait(&mut self, timeout: Option<Duration>, events: &mut Vec<PollEvent>) -> io::Result<()>; }`
  - `pub(crate) struct UnixReactor<H: Handler>` with `new(handler)`, `listen(addr)`, `connect(addr) -> io::Result<ConnId>`, `run_until(&mut self, shutdown: &Wake)`, `local_addr()`, and `spawn_for_test() -> (SocketAddr, Shutdown, JoinHandle<io::Result<()>>)`
  - `EpollPoller::new() -> io::Result<Self>`, `KqueuePoller::new() -> io::Result<Self>`
  - Test helpers in `src/rt/test_support.rs`: `spawn_echo()`, `spawn_echo_on(addr: &str) -> io::Result<(SocketAddr, Shutdown, JoinHandle<io::Result<()>>)>`, and `struct Shutdown`. The control/backlog variants are added in Task 10.
- Edge-triggered epoll (`EPOLLET | EPOLLIN | EPOLLOUT | EPOLLRDHUP`), full drain to `EAGAIN`, one read buffer per conn, write flush on writable, close on EOF/error.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(target_os = "linux")]
#[test]
fn echo_over_loopback() {
    let (addr, shutdown, join) = spawn_echo();
    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(b"hello").unwrap();
    let mut buf = [0u8; 5];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"hello");
    shutdown.shutdown();
    join.join().unwrap().unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn read_partial_then_eagain() {
    let (addr, shutdown, join) = spawn_echo();
    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(b"abc").unwrap();
    std::thread::sleep(Duration::from_millis(20));
    client.write_all(b"de").unwrap();
    let mut buf = [0u8; 5];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"abcde");
    shutdown.shutdown();
    join.join().unwrap().unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn write_partial_then_writable() {
    let (addr, shutdown, join) = spawn_echo();
    let mut client = TcpStream::connect(addr).unwrap();
    let payload = vec![0x5au8; 1 << 20];
    client.write_all(&payload).unwrap();
    let mut echo = vec![0u8; payload.len()];
    client.read_exact(&mut echo).unwrap();
    assert_eq!(echo, payload);
    shutdown.shutdown();
    join.join().unwrap().unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn spurious_read_event_is_harmless() {
    let (mut a, mut b) = UnixStream::pair().unwrap();
    a.set_nonblocking(true).unwrap();
    let mut poller = EpollPoller::new().unwrap();
    poller.add(a.as_raw_fd(), ConnId { slot: 0, generation: 0 }, Interest::Read).unwrap();
    let mut events = Vec::new();
    poller.wait(Some(Duration::from_millis(5)), &mut events).unwrap();
    assert!(events.is_empty());
    b.write_all(b"x").unwrap();
    poller.wait(Some(Duration::from_millis(100)), &mut events).unwrap();
    assert_eq!(events.len(), 1);
    let mut buf = [0u8; 1];
    assert_eq!(a.read(&mut buf).unwrap(), 1);
    poller.wait(Some(Duration::from_millis(5)), &mut events).unwrap();
    assert!(events.is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn close_with_pending_read() {
    let (addr, shutdown, join) = spawn_echo();
    let first = TcpStream::connect(addr).unwrap();
    first.set_nonblocking(true).unwrap();
    first.write_all(b"x").unwrap();
    drop(first);
    let mut second = TcpStream::connect(addr).unwrap();
    second.write_all(b"ok").unwrap();
    let mut buf = [0u8; 2];
    second.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"ok");
    shutdown.shutdown();
    join.join().unwrap().unwrap();
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib rt::reactor::unix`
Expected: FAIL with unresolved `UnixReactor`.

- [ ] **Step 3: Implement epoll poller and Unix reactor**

`epoll_create1(EPOLL_CLOEXEC)`, `epoll_ctl` add/mod/del, `epoll_wait` with timeout computed from the timer heap capped at 1 s; `Wake` fd is registered as a special event. On readable: loop `read` into the connection's block until `EAGAIN` or block full; `0` closes; errors close. On writable: write pending blocks with `writev`/`write`, partial writes retained. Close order per runtime design. Remove interest while paused.

- [ ] **Step 4: Run tests and checks**

Run: `cargo test --lib rt::reactor::unix && cargo clippy --all-targets -- -D warnings`
Expected: PASS on Linux.

- [ ] **Step 5: Commit**

```bash
git add src/rt/poller src/rt/reactor/unix.rs src/rt/mod.rs
git commit -m "feat(rt): epoll poller and loopback echo reactor"
```

---

### Task 8: kqueue backend for macOS/FreeBSD

**Files:**
- Create: `src/rt/poller/kqueue.rs`
- Modify: `src/rt/poller/mod.rs`, `src/rt/reactor/unix.rs`

**Interfaces:**
- Produces: `KqueuePoller` implementing the Task 7 `Poller` trait; `UnixReactor` selects `EpollPoller` on Linux and `KqueuePoller` on macOS/FreeBSD via `cfg`.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
#[test]
fn echo_over_loopback_kqueue() {
    let (addr, shutdown, join) = spawn_echo();
    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(b"hello").unwrap();
    let mut buf = [0u8; 5];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"hello");
    shutdown.shutdown();
    join.join().unwrap().unwrap();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib rt::reactor::unix`
Expected: FAIL with unresolved `KqueuePoller` on macOS/FreeBSD.

- [ ] **Step 3: Implement kqueue poller**

`kqueue()` + `kevent` changelist for add/mod/remove, `EV_CLEAR` for read/write filters, `EVFILT_USER` or the pipe wake fd for wakeups (use the pipe from Task 4), timeout as `timespec`. Same drain-to-`EAGAIN` and close rules as epoll. Do not use `EVFILT_USER` for multishot; it is only a wake channel.

- [ ] **Step 4: Run test and checks**

Run: `cargo test --lib rt::reactor::unix && cargo clippy --all-targets -- -D warnings`
Expected: PASS on macOS/FreeBSD; Linux path still passes.

- [ ] **Step 5: Commit**

```bash
git add src/rt/poller/kqueue.rs src/rt/poller/mod.rs src/rt/reactor/unix.rs
git commit -m "feat(rt): kqueue backend for macOS and FreeBSD"
```

---

### Task 9: Windows IOCP reactor

**Files:**
- Create: `src/rt/reactor/iocp.rs`, `src/rt/poller/iocp.rs`
- Modify: `src/rt/mod.rs`, `src/rt/wake.rs`

**Interfaces:**
- Produces: `pub(crate) struct IocpReactor<H: Handler>` with the same `new`, `listen`, `spawn_for_test`, `run_until` surface as `UnixReactor`.
- One posted `WSARecv`/`WSASend` per connection, bounded `AcceptEx` pool, `ConnectEx` for outbound, `GetQueuedCompletionStatusEx` batching, sentinel wake via `PostQueuedCompletionStatus`.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(windows)]
#[test]
fn echo_over_loopback_iocp() {
    let (addr, shutdown, join) = spawn_echo();
    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(b"hello").unwrap();
    let mut buf = [0u8; 5];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"hello");
    shutdown.shutdown();
    join.join().unwrap().unwrap();
}

// Close-with-pending-I/O and stale-completion tests are added in Task 10, after the
// TestControl harness exists; they are not optional.
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib rt::reactor::iocp`
Expected: FAIL with unresolved `IocpReactor` (on Windows).

- [ ] **Step 3: Implement IOCP reactor**

Associate listener and client sockets with the IOCP; pre-post a bounded `AcceptEx` pool; post `WSARecv`/`WSASend`; `SO_UPDATE_ACCEPT_CONTEXT`/`SO_UPDATE_CONNECT_CONTEXT`; on close `CancelIoEx` and free the slot only after the aborted completion; generation-check every completion; batch with `GetQueuedCompletionStatusEx`. `Wake` uses a dedicated completion key sentinel.

- [ ] **Step 4: Run tests and checks**

Run: `cargo test --lib rt::reactor::iocp && cargo clippy --all-targets -- -D warnings`
Expected: PASS on Windows.

- [ ] **Step 5: Commit**

```bash
git add src/rt/reactor/iocp.rs src/rt/poller/iocp.rs src/rt/mod.rs src/rt/wake.rs
git commit -m "feat(rt): IOCP reactor for Windows"
```

---

### Task 10: Backpressure, close/cancel and 10k-idle budget

**Files:**
- Modify: `src/rt/reactor/mod.rs`, `src/rt/reactor/unix.rs`, `src/rt/reactor/iocp.rs`, `src/rt/conn.rs`, `src/rt/test_support.rs` (allocator/counter instrumentation only; harness already exists from Task 7)
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Produces: `Conn::pause_read/resume_read` wired into both reactors; `ReactorCore::close(ConnId, reason)`; `TestControl { pause, resume, close_no_wait, mark_connection_reusable, deliver_stale, wait_for_read_bytes, wait_for_connections, total_read_bytes, first_connection, last_connection, peer_addr, local_addr, pending_ops }`; `spawn_echo_with_control()`, `spawn_echo_with_control_on(addr: &str)`, `spawn_echo_with_backlog()`; `test_support::{library_bytes, raise_nofile, wait_for_connections}`; library-owned byte counters; budget/backpressure assertions.
- Adds tests: `pause_stops_reads_until_resume`, `close_cancels_pending_operation`, `idle_10k_alloc_budget`, `stale_completion_after_reuse_is_ignored` (Windows), `cancel_io_on_close` (Windows).

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn pause_stops_reads_until_resume() {
    let (addr, shutdown, join, control) = spawn_echo_with_control();
    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(&vec![7u8; 64 * 1024]).unwrap();
    assert!(control.wait_for_read_bytes(1, Duration::from_secs(2)));
    control.pause();
    let paused = control.total_read_bytes();
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(control.total_read_bytes(), paused, "reactor kept reading while paused");
    client.write_all(&vec![8u8; 16 * 1024]).unwrap();
    control.resume();
    assert!(control.wait_for_read_bytes(paused + 16 * 1024, Duration::from_secs(2)));
    drop(client);
    shutdown.shutdown();
    join.join().unwrap().unwrap();
}

#[test]
fn close_cancels_pending_operation() {
    let (addr, shutdown, join, control) = spawn_echo_with_control();
    let client = TcpStream::connect(addr).unwrap();
    assert!(control.wait_for_connections(1, Duration::from_secs(2)));
    let id = control.first_connection();
    control.close(id);
    assert!(control.wait_for_connections(0, Duration::from_secs(2)));
    assert_eq!(control.pending_ops(id), 0);
    drop(client);

    let mut second = TcpStream::connect(addr).unwrap();
    second.write_all(b"ok").unwrap();
    let mut buf = [0u8; 2];
    second.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"ok");
    shutdown.shutdown();
    join.join().unwrap().unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn accept_ipv4_ipv6() {
    let (addr4, shutdown4, join4, control4) = spawn_echo_with_control_on("127.0.0.1:0").unwrap();
    let mut c4 = TcpStream::connect(addr4).unwrap();
    assert!(control4.wait_for_connections(1, Duration::from_secs(2)));
    let id4 = control4.first_connection();
    assert_eq!(control4.local_addr(id4), addr4);
    assert_eq!(control4.peer_addr(id4), c4.local_addr().unwrap());
    c4.write_all(b"v4").unwrap();
    let mut b4 = [0u8; 2]; c4.read_exact(&mut b4).unwrap(); assert_eq!(&b4, b"v4");
    shutdown4.shutdown(); join4.join().unwrap().unwrap();

    if let Ok((addr6, shutdown6, join6, control6)) = spawn_echo_with_control_on("[::1]:0") {
        let mut c6 = TcpStream::connect(addr6).unwrap();
        assert!(control6.wait_for_connections(1, Duration::from_secs(2)));
        let id6 = control6.first_connection();
        assert_eq!(control6.local_addr(id6), addr6);
        assert_eq!(control6.peer_addr(id6), c6.local_addr().unwrap());
        c6.write_all(b"v6").unwrap();
        let mut b6 = [0u8; 2]; c6.read_exact(&mut b6).unwrap(); assert_eq!(&b6, b"v6");
        shutdown6.shutdown(); join6.join().unwrap().unwrap();
    }
}

#[test]
fn idle_10k_alloc_budget() {
    test_support::raise_nofile(65_536);
    let before = test_support::library_bytes();
    let (addr, shutdown, join) = spawn_echo_with_backlog(1_024);
    let clients: Vec<TcpStream> = (0..10_000)
        .map(|_| TcpStream::connect(addr).unwrap())
        .collect();
    assert!(test_support::wait_for_connections(10_000, Duration::from_secs(5)));
    let after = test_support::library_bytes();
    let per_conn = (after - before) / 10_000;
    assert!(per_conn < 1024, "library allocated {per_conn} bytes per idle connection");
    drop(clients);
    shutdown.shutdown();
    join.join().unwrap().unwrap();
}

#[cfg(windows)]
#[test]
fn stale_completion_after_reuse_is_ignored() {
    let (addr, shutdown, join, control) = spawn_echo_with_control();
    let first = TcpStream::connect(addr).unwrap();
    assert!(control.wait_for_connections(1, Duration::from_secs(2)));
    let old = control.first_connection();
    control.close_no_wait(old); // posts CancelIoEx but leaves the completion to be delivered
    control.mark_connection_reusable(old); // test-only: free the slot while the completion is pending
    let mut second = TcpStream::connect(addr).unwrap();
    assert!(control.wait_for_connections(1, Duration::from_secs(2)));
    let new = control.last_connection();
    assert_eq!(new.slot, old.slot, "test did not reuse the old slot");
    control.deliver_stale(old); // force the old completion after the new connection exists
    second.write_all(b"new").unwrap();
    let mut buf = [0u8; 3];
    second.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"new", "stale completion affected the reused slot");
    drop(first);
    shutdown.shutdown();
    join.join().unwrap().unwrap();
}

#[cfg(windows)]
#[test]
fn cancel_io_on_close() {
    let (addr, shutdown, join, control) = spawn_echo_with_control();
    let client = TcpStream::connect(addr).unwrap();
    assert!(control.wait_for_connections(1, Duration::from_secs(2)));
    let id = control.first_connection();
    control.close(id);
    assert!(control.wait_for_connections(0, Duration::from_secs(2)));
    assert_eq!(control.pending_ops(id), 0);
    drop(client);
    shutdown.shutdown();
    join.join().unwrap().unwrap();
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib rt::reactor idle_10k_alloc_budget`
Expected: FAIL (no `TestControl`, pause wiring or library-byte counters).

- [ ] **Step 3: Wire backpressure, close, and the budget test**

Pause means clearing read interest (readiness) or not reposting the recv (IOCP); resume when the outbound queue falls below the low-water mark. `TestControl` is a test-only handle into the reactor; its `pending_ops(id)` makes the Windows close wait observable. Close order follows the runtime design; Windows waits for aborted completions. `test_support::library_bytes()` sums counters maintained by the reactor's slab and block pools, so 10,000 client `TcpStream`s and test allocations are excluded; `raise_nofile` and a 1,024 backlog make the 10,000-connection test stable. Add these tests to the CI matrix.

- [ ] **Step 4: Run all checks on every platform**

Run: `cargo test --all-targets && cargo fmt --check && cargo clippy --all-targets -- -D warnings`
Expected: PASS on Linux, macOS, Windows; FreeBSD job passes.

- [ ] **Step 5: Commit**

```bash
git add src/rt .github/workflows/ci.yml
git commit -m "feat(rt): backpressure, close/cancel and 10k-idle budget"
```

### Task 11: Outbound connect

**Files:**
- Modify: `src/rt/reactor/unix.rs`, `src/rt/reactor/iocp.rs`, `src/rt/handler.rs`, `src/rt/test_support.rs`

**Interfaces:**
- Produces: `UnixReactor::spawn_client_for_test() -> (TestClientHandle, Shutdown, JoinHandle<io::Result<()>>)`; test-only `TestClientHandle::connect(&self, addr: SocketAddr) -> io::Result<ConnId>` (enqueues and blocks the test thread until completion), `TestClientHandle::stats(&self) -> TestClientStats`, `TestClientHandle::wait_until(&self, timeout: Duration, pred: impl Fn(&TestClientStats) -> bool) -> bool`, `TestClientStats { connections: usize, echoed: bool }`; `Handler::on_connect` (default no-op). Windows `IocpReactor` provides the same test surface via `ConnectEx`. These are test-only names; the public `ClientHandle`/`ClientStats` do not exist until M2.
- `ConnectHandler` writes `ping` in `on_connect`, asserts `pong` in `on_read`, sets `echoed`, then closes.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(unix)]
#[test]
fn connect_refused_closes_socket() {
    let dead = {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let a = l.local_addr().unwrap();
        drop(l);
        a
    };
    let (handle, shutdown, join) = UnixReactor::<EchoHandler>::spawn_client_for_test();
    let err = handle.connect(dead).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
    assert_eq!(handle.stats().connections, 0);
    shutdown.shutdown();
    join.join().unwrap().unwrap();
}

#[cfg(unix)]
#[test]
fn connect_then_echo() {
    let (addr, shutdown, join) = spawn_echo();
    let (handle, _s, cjoin) = UnixReactor::<ConnectHandler>::spawn_client_for_test();
    handle.connect(addr).unwrap();
    assert!(handle.wait_until(Duration::from_secs(2), |s| s.echoed));
    shutdown.shutdown();
    cjoin.join().unwrap().unwrap();
}

#[cfg(unix)]
#[test]
fn connect_then_echo_ipv6() {
    let Ok((addr, shutdown, join)) = spawn_echo_on("[::1]:0") else { return; };
    let (handle, _s, cjoin) = UnixReactor::<ConnectHandler>::spawn_client_for_test();
    handle.connect(addr).unwrap();
    assert!(handle.wait_until(Duration::from_secs(2), |s| s.echoed));
    shutdown.shutdown();
    cjoin.join().unwrap().unwrap();
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib rt::reactor connect_`
Expected: FAIL with unresolved `connect`/`ConnectHandler`.

- [ ] **Step 3: Implement outbound connect**

Unix: create a nonblocking socket, `connect`, register `WRITE`, on writable check `SO_ERROR`, call `Handler::on_connect`; failure closes/reclaims the slot. Windows: `ConnectEx` with `SO_UPDATE_CONNECT_CONTEXT`, same handler callback and cancellation rules. No blocking `connect`.

- [ ] **Step 4: Run tests and checks**

Run: `cargo test --lib rt::reactor connect_ && cargo clippy --all-targets -- -D warnings`
Expected: PASS on Unix and Windows.

- [ ] **Step 5: Commit**

```bash
git add src/rt/reactor src/rt/handler.rs src/rt/test_support.rs
git commit -m "feat(rt): nonblocking outbound connect"
```

M1 is complete when: echo works on all four CI platforms; outbound connect (success, refused, IPv4/IPv6) is tested; pause/resume and close/cancel tests pass; 10k idle connections per reactor stay under the allocation budget; no reactor path performs a blocking socket call (verified by code review of `src/rt/reactor/*` and `src/rt/poller/*`); `cargo fmt`/`clippy`/`test` green everywhere.
