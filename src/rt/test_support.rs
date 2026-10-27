//! Test-only loopback harness and reactor instrumentation.
//!
//! `spawn_echo` starts a reactor on a background thread and returns its bound
//! address, a [`Shutdown`] handle and the thread's join handle. The
//! `*_with_control` variants also return a [`TestControl`], a thread-safe handle
//! the test can use to pause/resume reads, close a connection, observe
//! per-reactor byte counters and inspect connection addresses. The
//! `*_with_backlog` variant binds a large listen backlog for the 10k-idle test.
//!
//! This module also owns the process-wide library-byte counter sampled by
//! `idle_10k_alloc_budget`. Only reactor-owned allocations (the connection slab
//! and the block pool) report into it, so client sockets, test buffers and
//! standard-library allocations are excluded.
//!
//! The harness is `cfg(test)` only: M1 has no public runtime API.

#![cfg(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "freebsd",
    windows
))]
// The harness is cross-platform but each platform's tests exercise a subset:
// the Linux-only lifecycle tests, for example, use pause/counters that the
// Windows close/stale tests do not. Dead code is expected here, not rot.
#![allow(dead_code)]

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::rt::handler::EchoHandler;
#[cfg(windows)]
use crate::rt::reactor::iocp::IocpReactor;
#[cfg(unix)]
use crate::rt::reactor::unix::UnixReactor;
use crate::rt::slab::ConnId;
use crate::rt::wake::Wake;

/// Bytes currently owned by reactor slabs and block pools, process-wide.
static LIBRARY_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Live connections across every reactor in this test process.
static LIVE_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);

/// Adjusts the library-owned byte counter. Signed so freed capacity subtracts.
pub(crate) fn add_library_bytes(delta: isize) {
    adjust_saturating(&LIBRARY_BYTES, delta);
}

/// Applies a signed delta without ever wrapping the unsigned counter.
fn adjust_saturating(counter: &AtomicUsize, delta: isize) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add_signed(delta);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

/// Bytes currently owned by reactor slabs and block pools.
///
/// Updated by the slab and pool when they grow and when they are dropped. It
/// deliberately excludes allocations made by tests and by `std::net` clients.
pub(crate) fn library_bytes() -> usize {
    LIBRARY_BYTES.load(Ordering::Relaxed)
}

/// Records that a reactor inserted a connection into its slab.
pub(crate) fn note_connection_opened() {
    LIVE_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
}

/// Records that a reactor removed a connection from its slab.
pub(crate) fn note_connection_closed() {
    adjust_saturating(&LIVE_CONNECTIONS, -1);
}

/// Waits until at least `expected` connections are live across all reactors.
///
/// The 10k-idle test uses this without a control handle; it is process-wide, so
/// concurrently running tests can only make it observe the threshold earlier.
pub(crate) fn wait_for_connections(expected: usize, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if LIVE_CONNECTIONS.load(Ordering::Relaxed) >= expected {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Raises the process soft open-file limit to at least `want`.
///
/// Best-effort and capped by the hard limit: the 10k-idle test needs one file
/// descriptor per accepted socket plus the clients. Windows has no `RLIMIT`.
pub(crate) fn raise_nofile(want: u64) {
    #[cfg(unix)]
    {
        // SAFETY: `getrlimit`/`setrlimit` only read and write the initialized
        // `rlimit` value passed by pointer.
        unsafe {
            let mut limit: libc::rlimit = std::mem::zeroed();
            if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
                return;
            }
            let want = want as libc::rlim_t;
            if limit.rlim_cur < want {
                limit.rlim_cur = want.min(limit.rlim_max);
                let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &limit);
            }
        }
    }
    #[cfg(windows)]
    {
        let _ = want;
    }
}

/// Stops a reactor thread spawned by the harness.
pub(crate) struct Shutdown {
    wake: Arc<Wake>,
    /// Present for control spawns; shutdown must also set the stop flag the
    /// reactor checks when it dequeues a wake sentinel.
    channel: Option<Arc<ControlChannel>>,
}

impl Shutdown {
    /// Wraps the wake descriptor the reactor polls.
    pub(crate) fn new(wake: Arc<Wake>) -> Self {
        Self {
            wake,
            channel: None,
        }
    }

    /// Wraps the wake descriptor plus the control channel it also serves.
    pub(crate) fn with_control(wake: Arc<Wake>, channel: Arc<ControlChannel>) -> Self {
        Self {
            wake,
            channel: Some(channel),
        }
    }

    /// Requests shutdown. Safe to call more than once; wakes coalesce.
    pub(crate) fn shutdown(&self) {
        if let Some(channel) = &self.channel {
            channel.stop.store(true, Ordering::SeqCst);
        }
        let _ = self.wake.wake();
    }
}

/// Result of a plain harness spawn: address, shutdown and join handle.
pub(crate) type Spawned = (SocketAddr, Shutdown, JoinHandle<io::Result<()>>);

/// Result of a control harness spawn.
pub(crate) type SpawnedWithControl = (
    SocketAddr,
    Shutdown,
    JoinHandle<io::Result<()>>,
    TestControl,
);

/// Reactor-internal spawn result; the control handle is optional until asked.
pub(crate) type SpawnedInternal = (
    SocketAddr,
    Shutdown,
    JoinHandle<io::Result<()>>,
    Option<TestControl>,
);

/// One record of a live connection, kept so the test can inspect it.
#[derive(Clone)]
struct ConnRecord {
    id: ConnId,
    local: SocketAddr,
    peer: SocketAddr,
}

/// Reply to a synchronous control command.
pub(crate) enum Reply {
    /// The command was applied.
    Done,
    /// Result of a query command.
    Count(usize),
}

/// A command the test thread hands to the reactor thread.
pub(crate) struct Command {
    pub(crate) kind: CommandKind,
    pub(crate) ack: mpsc::Sender<Reply>,
}

/// Commands the reactor applies on its own thread.
pub(crate) enum CommandKind {
    /// Pause reads on every live connection.
    Pause,
    /// Clear the explicit read pause on every live connection.
    Resume,
    /// Close a connection through the reactor's normal close path.
    Close(ConnId),
    /// Report the number of outstanding kernel operations for a connection.
    PendingOps(ConnId),
    /// Windows: start a close and cancel posted operations without waiting.
    #[cfg(windows)]
    CloseNoWait(ConnId),
    /// Windows: free the slab slot while its canceled completions are pending.
    #[cfg(windows)]
    MarkReusable(ConnId),
    /// Windows: deliver a stale completion for an id whose slot was reused.
    #[cfg(windows)]
    DeliverStale(ConnId),
}

/// Shared state between a [`TestControl`], its reactor and its [`Shutdown`].
pub(crate) struct ControlChannel {
    wake: Arc<Wake>,
    stop: AtomicBool,
    commands: Mutex<Vec<Command>>,
    conns: Mutex<Vec<ConnRecord>>,
    read_bytes: AtomicU64,
}

impl ControlChannel {
    /// Builds a channel that wakes `wake`.
    pub(crate) fn new(wake: Arc<Wake>) -> Arc<Self> {
        Arc::new(Self {
            wake,
            stop: AtomicBool::new(false),
            commands: Mutex::new(Vec::new()),
            conns: Mutex::new(Vec::new()),
            read_bytes: AtomicU64::new(0),
        })
    }

    /// Whether shutdown was requested.
    pub(crate) fn stop(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    /// Takes every queued command, leaving the queue empty.
    pub(crate) fn take_commands(&self) -> Vec<Command> {
        std::mem::take(&mut *self.lock_commands())
    }

    /// Wakes the reactor so it drains the command queue.
    pub(crate) fn wake(&self) {
        let _ = self.wake.wake();
    }

    /// Drains the wake descriptor. The reactor must do this before waiting
    /// again, or an edge-triggered poller would never report later commands.
    #[cfg(unix)]
    pub(crate) fn drain_wake(&self) -> io::Result<bool> {
        self.wake.drain()
    }

    /// Records a newly inserted connection.
    pub(crate) fn note_open(&self, id: ConnId, local: SocketAddr, peer: SocketAddr) {
        self.lock_conns().push(ConnRecord { id, local, peer });
    }

    /// Records that a connection was removed.
    pub(crate) fn note_closed(&self, id: ConnId) {
        self.lock_conns().retain(|record| record.id != id);
    }

    /// Adds `n` delivered plaintext bytes to the reactor's read counter.
    pub(crate) fn note_read(&self, n: usize) {
        self.read_bytes.fetch_add(n as u64, Ordering::Relaxed);
    }

    /// Bytes the reactor has delivered to the handler so far.
    pub(crate) fn read_bytes(&self) -> u64 {
        self.read_bytes.load(Ordering::Relaxed)
    }

    /// Number of live connections in this reactor.
    pub(crate) fn connection_count(&self) -> usize {
        self.lock_conns().len()
    }

    /// The live connection ids in insertion order.
    pub(crate) fn ids(&self) -> Vec<ConnId> {
        self.lock_conns().iter().map(|record| record.id).collect()
    }

    fn first(&self) -> ConnId {
        self.lock_conns()
            .first()
            .expect("reactor has no live connection")
            .id
    }

    #[cfg(windows)]
    fn last(&self) -> ConnId {
        self.lock_conns()
            .last()
            .expect("reactor has no live connection")
            .id
    }

    fn local_addr(&self, id: ConnId) -> SocketAddr {
        self.lock_conns()
            .iter()
            .find(|record| record.id == id)
            .expect("no live connection with that id")
            .local
    }

    fn peer_addr(&self, id: ConnId) -> SocketAddr {
        self.lock_conns()
            .iter()
            .find(|record| record.id == id)
            .expect("no live connection with that id")
            .peer
    }

    fn lock_commands(&self) -> MutexGuard<'_, Vec<Command>> {
        self.commands.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn lock_conns(&self) -> MutexGuard<'_, Vec<ConnRecord>> {
        self.conns.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Test-only handle into a running reactor.
///
/// Commands are queued, the reactor is woken, and the call blocks until the
/// reactor has applied the command. That synchronous acknowledgement is what
/// makes "paused" observable: when `pause()` returns, no further read can start.
pub(crate) struct TestControl {
    channel: Arc<ControlChannel>,
}

impl TestControl {
    /// Wraps the channel shared with a control-spawned reactor.
    pub(crate) fn new(channel: Arc<ControlChannel>) -> Self {
        Self { channel }
    }

    /// Pauses reads on every live connection.
    pub(crate) fn pause(&self) {
        self.call(CommandKind::Pause);
    }

    /// Clears the explicit read pause on every live connection.
    pub(crate) fn resume(&self) {
        self.call(CommandKind::Resume);
    }

    /// Closes a connection through the reactor's normal close path.
    pub(crate) fn close(&self, id: ConnId) {
        self.call(CommandKind::Close(id));
    }

    /// Windows-only: closes and cancels posted operations without waiting.
    #[cfg(windows)]
    pub(crate) fn close_no_wait(&self, id: ConnId) {
        self.call(CommandKind::CloseNoWait(id));
    }

    /// Windows-only: frees the slab slot while canceled completions are still
    /// pending, so the next accept can reuse it.
    #[cfg(windows)]
    pub(crate) fn mark_connection_reusable(&self, id: ConnId) {
        self.call(CommandKind::MarkReusable(id));
    }

    /// Windows-only: forces a stale completion for `id` through the reactor.
    #[cfg(windows)]
    pub(crate) fn deliver_stale(&self, id: ConnId) {
        self.call(CommandKind::DeliverStale(id));
    }

    /// Number of outstanding operations for `id` (always zero without IOCP).
    pub(crate) fn pending_ops(&self, id: ConnId) -> usize {
        match self.call(CommandKind::PendingOps(id)) {
            Reply::Count(count) => count,
            Reply::Done => 0,
        }
    }

    /// Plaintext bytes the reactor has delivered to the handler.
    pub(crate) fn total_read_bytes(&self) -> u64 {
        self.channel.read_bytes()
    }

    /// Waits until at least `bytes` have been delivered to the handler.
    pub(crate) fn wait_for_read_bytes(&self, bytes: u64, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.channel.read_bytes() >= bytes {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Waits until exactly `count` connections are live in this reactor.
    pub(crate) fn wait_for_connections(&self, count: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.channel.connection_count() == count {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// The oldest live connection.
    pub(crate) fn first_connection(&self) -> ConnId {
        self.channel.first()
    }

    /// The newest live connection.
    #[cfg(windows)]
    pub(crate) fn last_connection(&self) -> ConnId {
        self.channel.last()
    }

    /// Local address cached for `id`.
    pub(crate) fn local_addr(&self, id: ConnId) -> SocketAddr {
        self.channel.local_addr(id)
    }

    /// Peer address cached for `id`.
    pub(crate) fn peer_addr(&self, id: ConnId) -> SocketAddr {
        self.channel.peer_addr(id)
    }

    /// Queues one command, wakes the reactor and waits for acknowledgement.
    fn call(&self, kind: CommandKind) -> Reply {
        let (ack, wait) = mpsc::channel();
        self.channel.lock_commands().push(Command { kind, ack });
        self.channel.wake();
        wait.recv_timeout(Duration::from_secs(5))
            .expect("reactor stopped handling control commands")
    }
}

/// Starts an echo reactor on an ephemeral `127.0.0.1` port.
pub(crate) fn spawn_echo() -> Spawned {
    #[cfg(unix)]
    {
        UnixReactor::<EchoHandler>::spawn_for_test()
    }
    #[cfg(windows)]
    {
        IocpReactor::<EchoHandler>::spawn_for_test()
    }
}

/// Starts an echo reactor bound to `addr`.
pub(crate) fn spawn_echo_on(addr: &str) -> io::Result<Spawned> {
    #[cfg(unix)]
    {
        UnixReactor::<EchoHandler>::new(EchoHandler).spawn_for_test_on(addr)
    }
    #[cfg(windows)]
    {
        IocpReactor::<EchoHandler>::new(EchoHandler).spawn_for_test_on(addr)
    }
}

/// Starts an echo reactor with a [`TestControl`] handle.
pub(crate) fn spawn_echo_with_control() -> SpawnedWithControl {
    spawn_echo_with_control_on("127.0.0.1:0").expect("bind loopback echo listener")
}

/// Starts an echo reactor bound to `addr` with a [`TestControl`] handle.
pub(crate) fn spawn_echo_with_control_on(addr: &str) -> io::Result<SpawnedWithControl> {
    #[cfg(unix)]
    {
        UnixReactor::<EchoHandler>::new(EchoHandler).spawn_with_control_on(addr)
    }
    #[cfg(windows)]
    {
        IocpReactor::<EchoHandler>::new(EchoHandler).spawn_with_control_on(addr)
    }
}

/// Starts an echo reactor on `127.0.0.1:0` with a `backlog` listen queue.
pub(crate) fn spawn_echo_with_backlog(backlog: usize) -> Spawned {
    #[cfg(unix)]
    {
        UnixReactor::<EchoHandler>::new(EchoHandler)
            .spawn_with_backlog_on("127.0.0.1:0", backlog)
            .expect("bind loopback echo listener")
    }
    #[cfg(windows)]
    {
        IocpReactor::<EchoHandler>::new(EchoHandler)
            .spawn_with_backlog_on("127.0.0.1:0", backlog)
            .expect("bind loopback echo listener")
    }
}
