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

use crate::rt::conn::Conn;
use crate::rt::handler::{EchoHandler, Handler};
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
    /// Result of an asynchronous outbound connect.
    Connect(io::Result<ConnId>),
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
    /// Start a nonblocking outbound connect; the reply arrives on completion.
    Connect(SocketAddr),
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

    /// Windows-only: cancels posted operations and frees the connection slot
    /// before returning. Returns how many operations were still posted when the
    /// slot was freed, so the stale-completion test can assert the race it
    /// intends to cover.
    #[cfg(windows)]
    pub(crate) fn close_no_wait(&self, id: ConnId) -> usize {
        match self.call(CommandKind::CloseNoWait(id)) {
            Reply::Count(count) => count,
            Reply::Done | Reply::Connect(_) => 0,
        }
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
            Reply::Done | Reply::Connect(_) => 0,
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

/// Test-only state a client handler shares with its [`TestClientHandle`].
#[derive(Default)]
pub(crate) struct ClientState {
    /// Set by [`ConnectHandler`] once its `ping` came back from the peer.
    echoed: AtomicBool,
}

/// A handler that can be built without arguments for a client-test reactor.
///
/// The reactor constructs the handler itself, so the state the test thread
/// observes via [`TestClientHandle`] has to come out of the same construction.
pub(crate) trait SpawnClient: Handler + Sized + Send + 'static {
    /// Builds the handler and a handle to the state it reports.
    fn new_client() -> (Self, Arc<ClientState>);
}

impl SpawnClient for EchoHandler {
    fn new_client() -> (Self, Arc<ClientState>) {
        // The refused-connect test only needs a handler to satisfy the reactor;
        // it never observes client state.
        (EchoHandler, Arc::new(ClientState::default()))
    }
}

/// The outbound-connect handler: sends `ping`, expects the echo, then closes.
///
/// The peer in these tests is [`EchoHandler`], which reflects bytes rather than
/// translating them. The plan's "expects `pong`" assumed a transforming peer;
/// the observable contract here is the same round trip: the client sends one
/// marker and requires exactly that marker back before it reports `echoed`.
pub(crate) struct ConnectHandler {
    state: Arc<ClientState>,
    /// Bytes of the expected echo received so far.
    ///
    /// A stream read may deliver the four-byte marker split across
    /// completions (notably with `WSARecv`), so the handler must accumulate
    /// instead of matching each chunk.
    received: Vec<u8>,
}

impl SpawnClient for ConnectHandler {
    fn new_client() -> (Self, Arc<ClientState>) {
        let state = Arc::new(ClientState::default());
        (
            Self {
                state: Arc::clone(&state),
                received: Vec::new(),
            },
            state,
        )
    }
}

impl Handler for ConnectHandler {
    fn on_accept(&mut self, _conn: &mut Conn) -> io::Result<()> {
        Ok(())
    }

    fn on_connect(&mut self, conn: &mut Conn) -> io::Result<()> {
        self.received.clear();
        conn.enqueue(b"ping")
            .map_err(|_| io::Error::other("client outbound queue rejected the request bytes"))
    }

    fn on_read(&mut self, _conn: &mut Conn, data: &[u8]) -> io::Result<()> {
        self.received.extend_from_slice(data);
        if !b"ping".starts_with(&self.received) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected the echoed ping, received {:?}", self.received),
            ));
        }
        if self.received.len() < b"ping".len() {
            // A partial read is normal on a stream; wait for the rest.
            return Ok(());
        }
        self.state.echoed.store(true, Ordering::SeqCst);
        // The handler has no graceful close hook, and a deferred close would
        // never fire on Windows because nothing is queued. A terminal handler
        // error is the documented immediate-close path on both reactors; no
        // output is lost because the ping was already flushed.
        Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "connect test complete",
        ))
    }

    fn on_close(&mut self, _id: ConnId) {}
}

/// Snapshot of the outbound-connect test reactor's observable state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TestClientStats {
    /// Live connections in the client reactor.
    pub(crate) connections: usize,
    /// Whether [`ConnectHandler`] has received its echoed `ping` (or, on a
    /// failed connect, definitively not).
    pub(crate) echoed: bool,
}

/// Test-only client side of a reactor, modelled on the future `ClientHandle`.
///
/// `connect` enqueues the request and blocks the *test* thread until the
/// reactor's event loop reports completion; the reactor thread never blocks on
/// a socket.
pub(crate) struct TestClientHandle {
    channel: Arc<ControlChannel>,
    state: Arc<ClientState>,
}

impl TestClientHandle {
    /// Wraps the channel shared with a client-spawned reactor.
    pub(crate) fn new(channel: Arc<ControlChannel>, state: Arc<ClientState>) -> Self {
        Self { channel, state }
    }

    /// Starts a nonblocking connect and waits for its completion.
    pub(crate) fn connect(&self, addr: SocketAddr) -> io::Result<ConnId> {
        let (ack, wait) = mpsc::channel();
        self.channel.lock_commands().push(Command {
            kind: CommandKind::Connect(addr),
            ack,
        });
        self.channel.wake();
        match wait.recv_timeout(Duration::from_secs(5)) {
            Ok(Reply::Connect(result)) => result,
            Ok(_) => Err(io::Error::other(
                "reactor answered a connect with the wrong reply",
            )),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "reactor did not complete the connect",
            )),
        }
    }

    /// Current connection and echo state.
    pub(crate) fn stats(&self) -> TestClientStats {
        TestClientStats {
            connections: self.channel.connection_count(),
            echoed: self.state.echoed.load(Ordering::SeqCst),
        }
    }

    /// Polls [`Self::stats`] until `pred` accepts it or `timeout` elapses.
    pub(crate) fn wait_until(
        &self,
        timeout: Duration,
        pred: impl Fn(&TestClientStats) -> bool,
    ) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if pred(&self.stats()) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_handler_accumulates_a_split_echo() {
        let (mut handler, state) = ConnectHandler::new_client();
        let mut conn = Conn::new_for_test(64);
        handler.on_connect(&mut conn).unwrap();

        // The stream may deliver the four-byte echo in any chunking.
        assert!(handler.on_read(&mut conn, b"pi").is_ok());
        assert!(handler.on_read(&mut conn, b"n").is_ok());
        assert!(!state.echoed.load(Ordering::SeqCst));
        assert!(handler.on_read(&mut conn, b"g").is_err());
        assert!(state.echoed.load(Ordering::SeqCst));
    }

    #[test]
    fn connect_handler_rejects_unexpected_bytes() {
        let (mut handler, state) = ConnectHandler::new_client();
        let mut conn = Conn::new_for_test(64);
        handler.on_connect(&mut conn).unwrap();

        let err = handler.on_read(&mut conn, b"pong").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(!state.echoed.load(Ordering::SeqCst));
    }

    #[test]
    fn connect_handler_rejects_extra_bytes_after_the_marker() {
        let (mut handler, state) = ConnectHandler::new_client();
        let mut conn = Conn::new_for_test(64);
        handler.on_connect(&mut conn).unwrap();

        let err = handler.on_read(&mut conn, b"ping!").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(!state.echoed.load(Ordering::SeqCst));
    }
}
