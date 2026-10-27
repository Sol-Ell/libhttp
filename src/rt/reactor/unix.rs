//! Unix readiness reactor: edge-triggered epoll/kqueue, accept/read/write/close.
//!
//! The reactor owns a [`ReactorCore`] (slab, pool, timers, handler) and a
//! native [`Poller`] (`EpollPoller` on Linux, `KqueuePoller` on macOS and
//! FreeBSD). Its loop performs every socket syscall on the owning
//! thread and then hands the shared lifecycle decisions to the core:
//!
//! - readable: drain the socket to `EAGAIN`, passing each chunk to the
//!   handler; `0` bytes is peer EOF;
//! - writable: gather queued blocks into `writev`, then record the written
//!   prefix with [`Conn::consume_written`] so a partial write keeps both its
//!   remainder and write interest;
//! - a drained queue calls [`ReactorCore::on_write_ready`] immediately, which
//!   completes a deferred EOF close even without a new readiness edge;
//! - outbound connect: a nonblocking socket is registered for WRITE; its
//!   writability or error edge is resolved from `SO_ERROR`, then
//!   [`Handler::on_connect`] runs and the socket joins the normal read/write
//!   flow. Failure closes the socket without ever entering the slab;
//! - any handler error (including an echo `WouldBlock` overflow) is terminal
//!   for that connection only; the loop continues serving the others.
//!
//! Readiness is edge-triggered, so a read is never retried without a new edge,
//! and only one interest set is registered per connection at a time.

#![cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]

use std::collections::HashMap;
use std::io::{self, Read};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use crate::rt::block::BLOCK_SIZE;
use crate::rt::conn::Conn;
#[cfg(test)]
use crate::rt::handler::EchoHandler;
use crate::rt::handler::Handler;
#[cfg(target_os = "linux")]
use crate::rt::poller::epoll::EpollPoller;
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
use crate::rt::poller::kqueue::KqueuePoller;
use crate::rt::poller::{Interest, PollEvent, Poller};
use crate::rt::reactor::ReactorCore;
use crate::rt::slab::ConnId;
use crate::rt::socket::OwnedSocket;
#[cfg(test)]
use crate::rt::test_support::{
    CommandKind, ControlChannel, Reply, Shutdown, SpawnClient, Spawned, SpawnedInternal,
    SpawnedWithControl, TestClientHandle, TestControl,
};
use crate::rt::wake::Wake;

/// Per-connection outbound cap. The echo handler reports `WouldBlock` beyond
/// it, which Task 10 replaces with carry/pause backpressure.
const WRITE_CAP: usize = BLOCK_SIZE * 16;

/// Sentinel id for the listening socket; the slab cannot produce this slot.
const LISTEN_ID: ConnId = ConnId {
    slot: u32::MAX - 1,
    generation: u32::MAX,
};

/// Sentinel id for the shutdown wake descriptor.
const WAKE_ID: ConnId = ConnId {
    slot: u32::MAX,
    generation: u32::MAX,
};

/// Generation tag reserved for the synthetic ids of in-flight connects.
///
/// A connecting socket needs an id before a [`Conn`] exists. These ids never
/// reach the slab, and the reserved generation keeps them distinct from every
/// slab id without reserving slots.
const CONNECT_GENERATION: u32 = u32::MAX - 2;

/// Upper bound on one poller wait, short enough for shutdown responsiveness.
const MAX_WAIT: Duration = Duration::from_secs(1);

/// Default listen backlog, matching the standard library's bind.
const DEFAULT_BACKLOG: usize = 1_024;

/// Blocks gathered into one `writev` call.
const MAX_IOVECS: usize = 64;

/// The readiness backend this target builds.
#[cfg(target_os = "linux")]
type NativePoller = EpollPoller;
/// The readiness backend this target builds.
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
type NativePoller = KqueuePoller;

/// The poller interest that matches a connection's current permission.
///
/// A partial write leaves `has_pending_write()` true, so the result keeps
/// write interest armed until the retained remainder is gone.
fn desired_interest(conn: &Conn) -> Option<Interest> {
    match (conn.is_paused(), conn.has_pending_write()) {
        // Reading is paused but output still needs to flush.
        (true, true) => Some(Interest::Write),
        // Reading is paused and the queue is empty: remove interest entirely
        // until an explicit resume.
        (true, false) => None,
        (false, true) => Some(Interest::Both),
        (false, false) => Some(Interest::Read),
    }
}

/// Outcome of creating a nonblocking connecting socket.
enum ConnectAttempt {
    /// The kernel completed the handshake before returning from `connect`.
    Connected(TcpStream),
    /// `connect` returned `EINPROGRESS`; writability signals completion.
    Pending(TcpStream),
}

/// Where a completed outbound connect reports its result.
///
/// The synchronous `connect` helper and the test client share the same start,
/// readiness and adoption path; only the waiter differs.
enum ConnectSink {
    /// The pre-loop synchronous helper waits on this channel.
    Sync(mpsc::Sender<io::Result<ConnId>>),
    /// The test handle's command acknowledgement.
    #[cfg(test)]
    Test(mpsc::Sender<Reply>),
}

impl ConnectSink {
    /// Delivers the final result exactly once.
    fn send(self, result: io::Result<ConnId>) {
        match self {
            Self::Sync(wait) => {
                let _ = wait.send(result);
            }
            #[cfg(test)]
            Self::Test(ack) => {
                let _ = ack.send(Reply::Connect(result));
            }
        }
    }
}

/// A socket whose nonblocking `connect` is still in flight.
struct PendingConnect {
    /// Owns the connecting socket; dropped on failure, adopted on success.
    stream: TcpStream,
    /// Receives the result when the WRITE edge reports completion.
    sink: ConnectSink,
}

/// Readiness reactor for Unix.
pub(crate) struct UnixReactor<H: Handler> {
    core: ReactorCore<H>,
    /// Created lazily so `new` cannot fail; `listen`, `connect` and
    /// `run_until` all establish it before registering anything.
    poller: Option<NativePoller>,
    listener: Option<TcpListener>,
    /// The interest currently registered for each id. `None` means the id was
    /// removed while paused; a later resume re-adds it.
    registrations: HashMap<ConnId, Interest>,
    /// In-flight nonblocking connects, keyed by their synthetic poller id.
    connecting: HashMap<ConnId, PendingConnect>,
    /// Next synthetic slot for [`Self::connecting`]; the generation is fixed.
    next_connect_id: u32,
    /// Test-only command channel installed by the harness.
    #[cfg(test)]
    control: Option<Arc<ControlChannel>>,
}

impl<H: Handler> UnixReactor<H> {
    /// Builds a reactor with an empty slab, pool, timer heap and poller.
    pub(crate) fn new(handler: H) -> Self {
        Self {
            core: ReactorCore::new(handler),
            poller: None,
            listener: None,
            registrations: HashMap::new(),
            connecting: HashMap::new(),
            next_connect_id: 0,
            #[cfg(test)]
            control: None,
        }
    }

    /// Binds a nonblocking listener to `addr` and registers it for accepting.
    pub(crate) fn listen(&mut self, addr: &str) -> io::Result<()> {
        self.listen_with_backlog(addr, DEFAULT_BACKLOG)
    }

    /// Binds a listener with an explicit kernel backlog.
    ///
    /// The standard library's bind fixes the backlog, so the 10k-idle harness
    /// re-issues `listen(2)` on the bound descriptor. `listen` on a listening
    /// socket only updates the queue limit.
    pub(crate) fn listen_with_backlog(&mut self, addr: &str, backlog: usize) -> io::Result<()> {
        if self.listener.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "reactor already has a listener",
            ));
        }

        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let fd = listener.as_raw_fd();
        let backlog = libc::c_int::try_from(backlog).unwrap_or(libc::c_int::MAX);
        // SAFETY: `fd` is a live listening socket; `listen(2)` only updates the
        // backlog bound and does not read or write memory.
        if unsafe { libc::listen(fd, backlog) } != 0 {
            return Err(io::Error::last_os_error());
        }
        self.register(LISTEN_ID, fd, Interest::Read)?;
        self.listener = Some(listener);
        Ok(())
    }

    /// Opens an outbound connection and reports it to the handler.
    ///
    /// The socket is nonblocking: `connect` returns immediately and the result
    /// is driven by the poller's WRITE readiness, exactly like the reactor
    /// loop. This pre-loop helper waits for that completion on its own; the
    /// test client uses the same path asynchronously.
    pub(crate) fn connect(&mut self, addr: &str) -> io::Result<ConnId> {
        let peer = addr.to_socket_addrs()?.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "address resolved to nothing")
        })?;
        let (wait, completed) = mpsc::channel();
        self.start_connect(peer, ConnectSink::Sync(wait));

        let mut events = Vec::with_capacity(64);
        loop {
            match completed.try_recv() {
                Ok(result) => return result,
                Err(mpsc::TryRecvError::Empty) => {}
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "connect completion channel closed",
                    ));
                }
            }
            self.poller_mut()?.wait(Some(MAX_WAIT), &mut events)?;
            for event in events.drain(..) {
                self.dispatch(event)?;
            }
            self.retry_carried_output()?;
            self.fire_timers();
        }
    }

    /// The bound listener address.
    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        match &self.listener {
            Some(listener) => listener.local_addr(),
            None => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "reactor is not listening",
            )),
        }
    }

    /// Runs the readiness loop until `shutdown` is signalled.
    ///
    /// The wake descriptor is edge-triggered and intentionally left readable:
    /// this reactor is single-use, and dropping it closes every connection.
    pub(crate) fn run_until(&mut self, shutdown: &Wake) -> io::Result<()> {
        let wake_fd = shutdown.raw_fd();
        self.register(WAKE_ID, wake_fd, Interest::Read)?;

        let mut events = Vec::with_capacity(64);
        loop {
            // Commands may have been queued before the loop started.
            #[cfg(test)]
            self.apply_control_commands();

            let timeout = self.poll_timeout();
            self.poller_mut()?.wait(Some(timeout), &mut events)?;

            if events.iter().any(|event| event.id == WAKE_ID) {
                #[cfg(test)]
                {
                    if let Some(control) = self.control.clone() {
                        if control.stop() {
                            return Ok(());
                        }
                        // The wake fd is edge-triggered: drain it or later
                        // commands would never raise another edge.
                        let _ = control.drain_wake();
                        self.apply_control_commands();
                    } else {
                        return Ok(());
                    }
                }
                #[cfg(not(test))]
                return Ok(());
            }

            for event in events.drain(..) {
                self.dispatch(event)?;
            }
            self.retry_carried_output()?;
            self.fire_timers();
        }
    }

    /// Retries carried handler output that the write path could not place.
    ///
    /// A retry after the queue drained may fail because the pool is temporarily
    /// exhausted; read interest is cleared while backpressured, so without this
    /// poll no further event would retry it. Called once per loop slice.
    fn retry_carried_output(&mut self) -> io::Result<()> {
        for id in self.core.retry_carry() {
            self.flush_pending(id)?;
            self.update_interest(id)?;
        }
        Ok(())
    }

    /// Applies every command the test control handle queued.
    #[cfg(test)]
    fn apply_control_commands(&mut self) {
        let Some(control) = self.control.clone() else {
            return;
        };
        for command in control.take_commands() {
            let reply = match command.kind {
                CommandKind::Pause => {
                    for id in control.ids() {
                        if let Some(conn) = self.core.conn_mut(id) {
                            conn.pause_read();
                        }
                        let _ = self.update_interest(id);
                    }
                    Reply::Done
                }
                CommandKind::Resume => {
                    for id in control.ids() {
                        if let Some(conn) = self.core.conn_mut(id) {
                            conn.resume_read();
                        }
                        let _ = self.update_interest(id);
                    }
                    Reply::Done
                }
                CommandKind::Close(id) => {
                    self.close_conn(id);
                    Reply::Done
                }
                CommandKind::PendingOps(id) => {
                    // Readiness backends have no outstanding kernel operation.
                    let _ = id;
                    Reply::Count(0)
                }
                CommandKind::Connect(addr) => {
                    // Completion replies through the acknowledgement channel
                    // once the WRITE edge reports the result.
                    self.start_connect(addr, ConnectSink::Test(command.ack));
                    continue;
                }
            };
            let _ = command.ack.send(reply);
        }
    }

    /// Returns the poller, creating it on first use.
    fn poller_mut(&mut self) -> io::Result<&mut NativePoller> {
        if self.poller.is_none() {
            self.poller = Some(NativePoller::new()?);
        }
        Ok(self.poller.as_mut().expect("poller was just initialized"))
    }

    /// The socket descriptor for a live connection.
    fn conn_fd(&self, id: ConnId) -> Option<RawFd> {
        self.core.conn(id).map(|conn| conn.socket().as_raw_fd())
    }

    /// Registers `fd` under `id` unless an interest is already recorded.
    fn register(&mut self, id: ConnId, fd: RawFd, interest: Interest) -> io::Result<()> {
        if self.registrations.contains_key(&id) {
            return Ok(());
        }
        self.poller_mut()?.add(fd, id, interest)?;
        self.registrations.insert(id, interest);
        Ok(())
    }

    /// Removes the poller interest recorded for `id`, if any.
    fn unregister(&mut self, id: ConnId, fd: RawFd) {
        if self.registrations.remove(&id).is_some() {
            if let Some(poller) = self.poller.as_mut() {
                // A kernel-side removal failure (the socket may already be
                // closing) is not actionable.
                let _ = poller.remove(fd);
            }
        }
    }

    /// A synthetic id for an in-flight connect.
    fn next_connect_id(&mut self) -> ConnId {
        let id = ConnId {
            slot: self.next_connect_id,
            generation: CONNECT_GENERATION,
        };
        self.next_connect_id = self.next_connect_id.wrapping_add(1);
        id
    }

    /// Starts a nonblocking connect and arranges for `sink` to receive it.
    ///
    /// The reactor thread never blocks on the socket: a pending `EINPROGRESS`
    /// is registered for WRITE and completed from the readiness loop.
    fn start_connect(&mut self, addr: SocketAddr, sink: ConnectSink) {
        let attempt = match nonblocking_connect(addr) {
            Ok(attempt) => attempt,
            Err(err) => return sink.send(Err(err)),
        };
        match attempt {
            ConnectAttempt::Connected(stream) => self.adopt_connect(stream, sink),
            ConnectAttempt::Pending(stream) => {
                let fd = stream.as_raw_fd();
                let id = self.next_connect_id();
                if let Err(err) = self.register(id, fd, Interest::Write) {
                    // Dropping `stream` closes the failed socket.
                    return sink.send(Err(err));
                }
                self.connecting.insert(id, PendingConnect { stream, sink });
            }
        }
    }

    /// Completes a connect whose WRITE edge fired.
    fn connect_ready(&mut self, id: ConnId) {
        let Some(pending) = self.connecting.remove(&id) else {
            return;
        };
        let fd = pending.stream.as_raw_fd();
        self.unregister(id, fd);

        match socket_error(fd) {
            Ok(0) => self.adopt_connect(pending.stream, pending.sink),
            Ok(code) => pending.sink.send(Err(io::Error::from_raw_os_error(code))),
            Err(err) => pending.sink.send(Err(err)),
        }
    }

    /// Turns a completed connecting socket into a live connection.
    ///
    /// Every failure closes the socket and, when the connection had already
    /// entered the slab, reclaims its slot through the core.
    fn adopt_connect(&mut self, stream: TcpStream, sink: ConnectSink) {
        let fd = stream.as_raw_fd();
        let socket = match OwnedSocket::new(stream) {
            Ok(socket) => socket,
            Err(err) => return sink.send(Err(err)),
        };
        let conn = match Conn::new(socket, Arc::clone(self.core.pool()), WRITE_CAP) {
            Ok(conn) => conn,
            Err(err) => return sink.send(Err(err)),
        };
        let id = self.core.insert(conn);

        if let Err(err) = self.register(id, fd, Interest::Read) {
            self.core.close(id, "register-failed");
            return sink.send(Err(err));
        }

        if let Err(err) = self.core.on_connect(id) {
            // The core already closed the socket with the connection.
            self.registrations.remove(&id);
            return sink.send(Err(err));
        }

        // Send whatever the handler queued in `on_connect` before waiting for
        // the next readiness edge.
        if let Err(err) = self
            .flush_pending(id)
            .and_then(|()| self.update_interest(id))
        {
            self.close_conn(id);
            return sink.send(Err(err));
        }

        sink.send(Ok(id));
    }

    /// Time until the next timer, capped at [`MAX_WAIT`].
    fn poll_timeout(&self) -> Duration {
        let now = Instant::now();
        match self.core.timers().next_deadline() {
            Some(deadline) => deadline.saturating_duration_since(now).min(MAX_WAIT),
            None => MAX_WAIT,
        }
    }

    /// Discards expired timers. M1 has no timer callbacks yet.
    fn fire_timers(&mut self) {
        let _ = self.core.timers_mut().pop_expired(Instant::now());
    }

    /// Applies one readiness notification.
    fn dispatch(&mut self, event: PollEvent) -> io::Result<()> {
        let id = event.id;
        if id == WAKE_ID {
            return Ok(());
        }
        if id == LISTEN_ID {
            return self.accept_ready();
        }
        if self.connecting.contains_key(&id) {
            // The socket has no connection state yet: either the connect
            // finished or the readiness layer flagged an error. SO_ERROR is
            // the authoritative result in both cases.
            self.connect_ready(id);
            return Ok(());
        }

        if event.error.is_some() {
            // Terminal at the readiness layer: retrying the socket would only
            // surface the same error, so close without another syscall.
            self.close_conn(id);
            return Ok(());
        }

        if event.readable {
            self.handle_readable(id)?;
        }
        if event.writable && self.core.conn(id).is_some() {
            self.flush_pending(id)?;
        }
        self.update_interest(id)?;
        Ok(())
    }

    /// Accepts every connection queued on the listener, then re-arms.
    fn accept_ready(&mut self) -> io::Result<()> {
        loop {
            let accepted = match self.listener.as_ref() {
                Some(listener) => listener.accept(),
                None => return Ok(()),
            };
            let (stream, _peer) = match accepted {
                Ok(pair) => pair,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            };

            let socket = match OwnedSocket::new(stream) {
                Ok(socket) => socket,
                Err(_) => continue,
            };
            let conn = match Conn::new(socket, Arc::clone(self.core.pool()), WRITE_CAP) {
                Ok(conn) => conn,
                Err(_) => continue,
            };
            let id = self.core.insert(conn);

            let fd = self
                .conn_fd(id)
                .expect("connection was just inserted into the slab");
            self.register(id, fd, Interest::Read)?;

            // A handler accept failure is terminal for this connection only:
            // the core has closed it and bumped the slot generation.
            if self.core.on_accept(id).is_err() {
                self.registrations.remove(&id);
                continue;
            }
            self.update_interest(id)?;
        }
    }

    /// Drains a readable connection until `EAGAIN`, echoing as it goes.
    fn handle_readable(&mut self, id: ConnId) -> io::Result<()> {
        let mut buf = [0u8; BLOCK_SIZE];

        loop {
            let read = {
                let Some(conn) = self.core.conn_mut(id) else {
                    return Ok(());
                };
                if conn.is_paused() {
                    return Ok(());
                }
                conn.socket_mut().read(&mut buf)
            };

            match read {
                Ok(0) => {
                    // Peer EOF: close now, or after the queued echo flushes.
                    self.core.on_eof(id);
                    self.flush_pending(id)?;
                    return Ok(());
                }
                Ok(n) => {
                    #[cfg(test)]
                    self.core.note_read(n);
                    if self.core.on_read_ready(id, &buf[..n]).is_err() {
                        // Handler failure (echo overflow included) is fatal
                        // for this connection; nothing is retried.
                        return Ok(());
                    }
                    self.flush_pending(id)?;
                    if self.core.conn(id).is_none_or(|conn| conn.is_paused()) {
                        return Ok(());
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    self.close_conn(id);
                    return Ok(());
                }
            }
        }
    }

    /// Writes queued blocks until the socket would block or the queue drains.
    ///
    /// A partial write is recorded in [`Conn`] so the unwritten remainder stays
    /// queued; only a fully drained queue calls
    /// [`ReactorCore::on_write_ready`], which may complete a deferred close.
    fn flush_pending(&mut self, id: ConnId) -> io::Result<()> {
        loop {
            let has_pending = self
                .core
                .conn(id)
                .is_some_and(|conn| conn.has_pending_write());
            if !has_pending {
                // The queue is empty, including on entry: this is the point
                // where a deferred EOF close runs and backpressure clears. The
                // handler may retry carried output into the empty queue here.
                self.core.on_write_ready(id)?;
                if self
                    .core
                    .conn(id)
                    .is_none_or(|conn| !conn.has_pending_write())
                {
                    return Ok(());
                }
            }

            let mut iovecs = [libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }; MAX_IOVECS];
            let (fd, iovec_len, batch_len) = {
                let conn = self
                    .core
                    .conn(id)
                    .expect("pending write implies a live connection");
                let fd = conn.socket().as_raw_fd();
                let mut iovec_len = 0;
                let mut batch_len = 0;
                for block in conn.pending_blocks() {
                    if iovec_len == MAX_IOVECS {
                        break;
                    }
                    iovecs[iovec_len] = libc::iovec {
                        iov_base: block.as_slice().as_ptr().cast_mut().cast(),
                        iov_len: block.len(),
                    };
                    iovec_len += 1;
                    batch_len += block.len();
                }
                (fd, iovec_len, batch_len)
            };
            debug_assert!(iovec_len > 0, "pending bytes imply a non-empty block");

            // SAFETY: every iovec points at a live, disjoint block range owned
            // by the connection, and the queue is not mutated between building
            // the vectors and this call, so the pointers stay valid. `writev`
            // only reads the memory, `fd` is the connection's live socket, and
            // `iovec_len` is within the initialized prefix of `iovecs`.
            let written = unsafe { libc::writev(fd, iovecs.as_ptr(), iovec_len as libc::c_int) };
            if written < 0 {
                let err = io::Error::last_os_error();
                match err.kind() {
                    io::ErrorKind::WouldBlock => return Ok(()),
                    io::ErrorKind::Interrupted => continue,
                    _ => {
                        self.close_conn(id);
                        return Ok(());
                    }
                }
            }

            let written = written as usize;
            if written == 0 {
                // No progress; wait for the next writable edge instead of
                // spinning on a ready-looking socket.
                return Ok(());
            }

            let drained = match self.core.conn_mut(id) {
                Some(conn) => {
                    conn.consume_written(written);
                    !conn.has_pending_write()
                }
                None => return Ok(()),
            };

            if drained {
                self.core.on_write_ready(id)?;
                // The handler may have retried a carried chunk into the queue;
                // write it now instead of waiting for another edge.
                if self
                    .core
                    .conn(id)
                    .is_some_and(|conn| conn.has_pending_write())
                {
                    continue;
                }
                return Ok(());
            }
            if written < batch_len {
                // Partial write: the remainder stays queued and write interest
                // stays armed until the next writable edge.
                return Ok(());
            }
            // The whole gathered batch went out; loop for any blocks beyond
            // MAX_IOVECS.
        }
    }

    /// Closes a connection after clearing its poller interest.
    fn close_conn(&mut self, id: ConnId) {
        match self.conn_fd(id) {
            Some(fd) => self.unregister(id, fd),
            None => {
                self.registrations.remove(&id);
            }
        }
        self.core.close(id, "reactor-close");
    }

    /// Reconciles the registered interest with the connection's state.
    fn update_interest(&mut self, id: ConnId) -> io::Result<()> {
        let desired = match self.core.conn(id) {
            Some(conn) => desired_interest(conn),
            None => {
                self.registrations.remove(&id);
                return Ok(());
            }
        };

        let current = self.registrations.get(&id).copied();
        if current == desired {
            return Ok(());
        }

        let fd = match self.conn_fd(id) {
            Some(fd) => fd,
            None => {
                self.registrations.remove(&id);
                return Ok(());
            }
        };

        match (current, desired) {
            (None, Some(interest)) => {
                self.poller_mut()?.add(fd, id, interest)?;
                self.registrations.insert(id, interest);
            }
            (Some(_), None) => {
                self.registrations.remove(&id);
                if let Some(poller) = self.poller.as_mut() {
                    let _ = poller.remove(fd);
                }
            }
            (Some(_), Some(interest)) => {
                self.poller_mut()?.modify(fd, id, interest)?;
                self.registrations.insert(id, interest);
            }
            (None, None) => {}
        }
        Ok(())
    }
}

#[cfg(test)]
impl<H: Handler + Send + 'static> UnixReactor<H> {
    /// Binds `addr`, then moves the reactor onto a background thread.
    ///
    /// The returned [`Shutdown`] wakes the loop; the join handle carries the
    /// loop's result.
    pub(crate) fn spawn_for_test_on(self, addr: &str) -> io::Result<Spawned> {
        let (addr, shutdown, join, _) = self.spawn_inner(addr, None, false)?;
        Ok((addr, shutdown, join))
    }

    /// Binds `addr`, then moves the reactor onto a background thread with a
    /// [`TestControl`] handle.
    pub(crate) fn spawn_with_control_on(self, addr: &str) -> io::Result<SpawnedWithControl> {
        let (addr, shutdown, join, control) = self.spawn_inner(addr, None, true)?;
        Ok((
            addr,
            shutdown,
            join,
            control.expect("control spawn always returns a handle"),
        ))
    }

    /// Binds `addr` with an explicit listen backlog and moves the reactor onto
    /// a background thread.
    pub(crate) fn spawn_with_backlog_on(self, addr: &str, backlog: usize) -> io::Result<Spawned> {
        let (addr, shutdown, join, _) = self.spawn_inner(addr, Some(backlog), false)?;
        Ok((addr, shutdown, join))
    }

    /// Shared spawn path for the plain, control and backlog harness variants.
    fn spawn_inner(
        mut self,
        addr: &str,
        backlog: Option<usize>,
        with_control: bool,
    ) -> io::Result<SpawnedInternal> {
        match backlog {
            Some(backlog) => self.listen_with_backlog(addr, backlog)?,
            None => self.listen(addr)?,
        }
        let local = self.local_addr()?;
        let wake = Arc::new(Wake::new()?);
        let (shutdown, control) = if with_control {
            let channel = ControlChannel::new(Arc::clone(&wake));
            self.core.attach_control(Arc::clone(&channel));
            self.control = Some(Arc::clone(&channel));
            (
                Shutdown::with_control(Arc::clone(&wake), Arc::clone(&channel)),
                Some(TestControl::new(channel)),
            )
        } else {
            (Shutdown::new(Arc::clone(&wake)), None)
        };
        let join = std::thread::spawn(move || self.run_until(&wake));
        Ok((local, shutdown, join, control))
    }
}

/// Creates a nonblocking socket and starts `connect(2)` on it.
///
/// A successful handshake is reported as [`ConnectAttempt::Connected`];
/// `EINPROGRESS` (the normal loopback and network case) is
/// [`ConnectAttempt::Pending`], to be completed from WRITE readiness.
fn nonblocking_connect(addr: SocketAddr) -> io::Result<ConnectAttempt> {
    let family = if addr.is_ipv4() {
        libc::AF_INET
    } else {
        libc::AF_INET6
    };
    // SAFETY: socket(2) takes no pointers; a negative result is checked before
    // the descriptor is wrapped.
    let fd = unsafe { libc::socket(family, libc::SOCK_STREAM, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` is a fresh, uniquely owned descriptor; the stream takes
    // ownership and closes it exactly once.
    let stream = unsafe { TcpStream::from_raw_fd(fd) };
    stream.set_nonblocking(true)?;
    set_cloexec(fd)?;

    let (address, len) = sockaddr(addr);
    // SAFETY: `address` is a fully initialized sockaddr of length `len`; the
    // call only reads that memory.
    let rc = unsafe { libc::connect(fd, std::ptr::from_ref(&address).cast(), len) };
    if rc == 0 {
        return Ok(ConnectAttempt::Connected(stream));
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EINPROGRESS) | Some(libc::EALREADY) => Ok(ConnectAttempt::Pending(stream)),
        _ => Err(err),
    }
}

/// Builds the sockaddr bytes for `addr` in a family-neutral buffer.
fn sockaddr(addr: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    // SAFETY: an all-zero sockaddr_storage is valid; the concrete struct is
    // written over it before the buffer is used.
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(v4) => {
            // SAFETY: an all-zero sockaddr_in is valid; every field is set below.
            let mut sin: libc::sockaddr_in = unsafe { std::mem::zeroed() };
            sin.sin_family = libc::AF_INET as libc::sa_family_t;
            sin.sin_port = v4.port().to_be();
            // The address bytes are stored in network order.
            sin.sin_addr = libc::in_addr {
                s_addr: u32::from_ne_bytes(v4.ip().octets()),
            };
            #[cfg(any(target_os = "macos", target_os = "freebsd"))]
            {
                sin.sin_len = std::mem::size_of::<libc::sockaddr_in>() as u8;
            }
            // SAFETY: sockaddr_in fits inside sockaddr_storage and `sin` is
            // fully initialized.
            unsafe {
                std::ptr::write(
                    std::ptr::from_mut(&mut storage).cast::<libc::sockaddr_in>(),
                    sin,
                );
            }
            std::mem::size_of::<libc::sockaddr_in>()
        }
        SocketAddr::V6(v6) => {
            // SAFETY: an all-zero sockaddr_in6 is valid; every field is set below.
            let mut sin6: libc::sockaddr_in6 = unsafe { std::mem::zeroed() };
            sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sin6.sin6_port = v6.port().to_be();
            sin6.sin6_addr = libc::in6_addr {
                s6_addr: v6.ip().octets(),
            };
            sin6.sin6_scope_id = v6.scope_id();
            #[cfg(any(target_os = "macos", target_os = "freebsd"))]
            {
                sin6.sin6_len = std::mem::size_of::<libc::sockaddr_in6>() as u8;
            }
            // SAFETY: sockaddr_in6 fits inside sockaddr_storage and `sin6` is
            // fully initialized.
            unsafe {
                std::ptr::write(
                    std::ptr::from_mut(&mut storage).cast::<libc::sockaddr_in6>(),
                    sin6,
                );
            }
            std::mem::size_of::<libc::sockaddr_in6>()
        }
    };
    (storage, len as libc::socklen_t)
}

/// Reads and clears `SO_ERROR`, the authoritative result of a nonblocking
/// connect once its WRITE edge fires.
fn socket_error(fd: RawFd) -> io::Result<libc::c_int> {
    let mut error: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `error` and `len` are valid out storage for SO_ERROR, and `fd` is
    // a live socket owned by the caller.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            std::ptr::from_mut(&mut error).cast(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(error)
}

/// Marks a descriptor close-on-exec. `TcpStream::from_raw_fd` does not.
fn set_cloexec(fd: RawFd) -> io::Result<()> {
    // SAFETY: fcntl(2) operates on the live descriptor with no pointers.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: as above; only the descriptor flags are changed.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
impl<H: Handler + SpawnClient + Send + 'static> UnixReactor<H> {
    /// Spawns a listenerless reactor for the outbound-connect tests.
    ///
    /// Connect requests arrive through the test control channel, so the test
    /// thread blocks until completion while the reactor thread stays
    /// nonblocking.
    pub(crate) fn spawn_client_for_test() -> (
        TestClientHandle,
        Shutdown,
        std::thread::JoinHandle<io::Result<()>>,
    ) {
        let (handler, state) = H::new_client();
        let mut reactor = Self::new(handler);
        let wake = Arc::new(Wake::new().expect("create the reactor wake"));
        let channel = ControlChannel::new(Arc::clone(&wake));
        reactor.core.attach_control(Arc::clone(&channel));
        reactor.control = Some(Arc::clone(&channel));
        let shutdown = Shutdown::with_control(Arc::clone(&wake), Arc::clone(&channel));
        let handle = TestClientHandle::new(Arc::clone(&channel), state);
        let join = std::thread::spawn(move || reactor.run_until(&wake));
        (handle, shutdown, join)
    }
}

#[cfg(test)]
impl UnixReactor<EchoHandler> {
    /// Starts an echo reactor on an ephemeral loopback port.
    pub(crate) fn spawn_for_test() -> (
        SocketAddr,
        Shutdown,
        std::thread::JoinHandle<io::Result<()>>,
    ) {
        Self::new(EchoHandler)
            .spawn_for_test_on("127.0.0.1:0")
            .expect("bind loopback echo listener")
    }
}

#[cfg(test)]
mod tests {
    use super::UnixReactor;
    #[cfg(target_os = "linux")]
    use super::desired_interest;
    use crate::rt::handler::EchoHandler;
    #[cfg(target_os = "linux")]
    use crate::rt::poller::epoll::EpollPoller;
    #[cfg(target_os = "linux")]
    use crate::rt::poller::{Interest, Poller};
    #[cfg(target_os = "linux")]
    use crate::rt::slab::ConnId;
    #[cfg(unix)]
    use crate::rt::test_support::spawn_echo_with_control;
    use crate::rt::test_support::{ConnectHandler, spawn_echo, spawn_echo_on};
    #[cfg(target_os = "linux")]
    use crate::rt::test_support::{
        library_bytes, raise_nofile, spawn_echo_with_backlog, spawn_echo_with_control_on,
        wait_for_connections,
    };
    use std::io::{self, Read, Write};
    use std::net::{TcpListener, TcpStream};
    #[cfg(target_os = "linux")]
    use std::os::fd::AsRawFd;
    #[cfg(target_os = "linux")]
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

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
        let payload = vec![0x5au8; 32 * 1024];
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
        poller
            .add(
                a.as_raw_fd(),
                ConnId {
                    slot: 0,
                    generation: 0,
                },
                Interest::Read,
            )
            .unwrap();
        let mut events = Vec::new();
        poller
            .wait(Some(Duration::from_millis(5)), &mut events)
            .unwrap();
        assert!(events.is_empty());
        b.write_all(b"x").unwrap();
        poller
            .wait(Some(Duration::from_millis(100)), &mut events)
            .unwrap();
        assert_eq!(events.len(), 1);
        let mut buf = [0u8; 1];
        assert_eq!(a.read(&mut buf).unwrap(), 1);
        poller
            .wait(Some(Duration::from_millis(5)), &mut events)
            .unwrap();
        assert!(events.is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn close_with_pending_read() {
        let (addr, shutdown, join) = spawn_echo();
        let mut first = TcpStream::connect(addr).unwrap();
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

    #[test]
    fn spawn_echo_on_binds_the_requested_loopback_address() {
        let (addr, shutdown, join) = spawn_echo_on("127.0.0.1:0").unwrap();
        assert!(addr.ip().is_loopback());
        assert_ne!(addr.port(), 0);
        shutdown.shutdown();
        join.join().unwrap().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn partial_write_keeps_write_interest_armed() {
        use crate::rt::block::BLOCK_SIZE;
        use crate::rt::conn::Conn;

        let mut conn = Conn::new_for_test(BLOCK_SIZE * 2);
        conn.enqueue(b"abc").unwrap();
        assert_eq!(desired_interest(&conn), Some(Interest::Both));

        conn.consume_written(2);
        assert!(conn.has_pending_write(), "a partial write retains the tail");
        assert_eq!(
            desired_interest(&conn),
            Some(Interest::Both),
            "EPOLLOUT must stay armed while the remainder is queued"
        );

        conn.consume_written(1);
        assert_eq!(desired_interest(&conn), Some(Interest::Read));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn paused_with_a_drained_queue_drops_interest() {
        use crate::rt::conn::Conn;

        let mut conn = Conn::new_for_test(64);
        conn.pause_read();
        assert_eq!(desired_interest(&conn), None);
    }

    #[cfg(unix)]
    #[test]
    fn pause_stops_reads_until_resume() {
        let (addr, shutdown, join, control) = spawn_echo_with_control();
        let mut client = TcpStream::connect(addr).unwrap();
        client.write_all(&vec![7u8; 64 * 1024]).unwrap();
        assert!(control.wait_for_read_bytes(1, Duration::from_secs(2)));
        control.pause();
        let paused = control.total_read_bytes();
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(
            control.total_read_bytes(),
            paused,
            "reactor kept reading while paused"
        );
        client.write_all(&vec![8u8; 16 * 1024]).unwrap();
        control.resume();
        assert!(control.wait_for_read_bytes(paused + 16 * 1024, Duration::from_secs(2)));
        drop(client);
        shutdown.shutdown();
        join.join().unwrap().unwrap();
    }

    #[cfg(unix)]
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
        let (addr4, shutdown4, join4, control4) =
            spawn_echo_with_control_on("127.0.0.1:0").unwrap();
        let mut c4 = TcpStream::connect(addr4).unwrap();
        assert!(control4.wait_for_connections(1, Duration::from_secs(2)));
        let id4 = control4.first_connection();
        assert_eq!(control4.local_addr(id4), addr4);
        assert_eq!(control4.peer_addr(id4), c4.local_addr().unwrap());
        c4.write_all(b"v4").unwrap();
        let mut b4 = [0u8; 2];
        c4.read_exact(&mut b4).unwrap();
        assert_eq!(&b4, b"v4");
        shutdown4.shutdown();
        join4.join().unwrap().unwrap();

        if let Ok((addr6, shutdown6, join6, control6)) = spawn_echo_with_control_on("[::1]:0") {
            let mut c6 = TcpStream::connect(addr6).unwrap();
            assert!(control6.wait_for_connections(1, Duration::from_secs(2)));
            let id6 = control6.first_connection();
            assert_eq!(control6.local_addr(id6), addr6);
            assert_eq!(control6.peer_addr(id6), c6.local_addr().unwrap());
            c6.write_all(b"v6").unwrap();
            let mut b6 = [0u8; 2];
            c6.read_exact(&mut b6).unwrap();
            assert_eq!(&b6, b"v6");
            shutdown6.shutdown();
            join6.join().unwrap().unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn connect_refused_closes_socket() {
        let dead = {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            drop(listener);
            addr
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
        let (handle, client_shutdown, client_join) =
            UnixReactor::<ConnectHandler>::spawn_client_for_test();
        handle.connect(addr).unwrap();
        assert!(handle.wait_until(Duration::from_secs(2), |s| s.echoed));
        assert!(
            handle.wait_until(Duration::from_secs(2), |s| s.connections == 0),
            "handler close must reclaim the client connection"
        );
        shutdown.shutdown();
        client_shutdown.shutdown();
        join.join().unwrap().unwrap();
        client_join.join().unwrap().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn connect_then_echo_ipv6() {
        let Ok((addr, shutdown, join)) = spawn_echo_on("[::1]:0") else {
            return;
        };
        let (handle, client_shutdown, client_join) =
            UnixReactor::<ConnectHandler>::spawn_client_for_test();
        handle.connect(addr).unwrap();
        assert!(handle.wait_until(Duration::from_secs(2), |s| s.echoed));
        assert!(
            handle.wait_until(Duration::from_secs(2), |s| s.connections == 0),
            "handler close must reclaim the client connection"
        );
        shutdown.shutdown();
        client_shutdown.shutdown();
        join.join().unwrap().unwrap();
        client_join.join().unwrap().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn connect_reports_a_live_connection() {
        let (addr, shutdown, join) = spawn_echo();
        let (handle, client_shutdown, client_join) =
            UnixReactor::<EchoHandler>::spawn_client_for_test();
        let _id = handle.connect(addr).unwrap();
        assert_eq!(handle.stats().connections, 1);
        shutdown.shutdown();
        client_shutdown.shutdown();
        join.join().unwrap().unwrap();
        client_join.join().unwrap().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn pre_loop_connect_uses_the_nonblocking_path() {
        let (addr, shutdown, join) = spawn_echo();
        let mut reactor = UnixReactor::<EchoHandler>::new(EchoHandler);
        let id = reactor.connect(&addr.to_string()).unwrap();
        assert_eq!(reactor.core.conn(id).unwrap().peer_addr(), addr);
        drop(reactor);
        shutdown.shutdown();
        join.join().unwrap().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn idle_10k_alloc_budget() {
        raise_nofile(65_536);
        let before = library_bytes();
        let (addr, shutdown, join) = spawn_echo_with_backlog(1_024);
        let clients: Vec<TcpStream> = (0..10_000)
            .map(|_| TcpStream::connect(addr).unwrap())
            .collect();
        assert!(wait_for_connections(10_000, Duration::from_secs(5)));
        let after = library_bytes();
        // Other tests may release reactor memory between the two samples, so
        // the delta is a lower bound on this reactor's growth.
        let per_conn = after.saturating_sub(before) / 10_000;
        assert!(
            per_conn < 1024,
            "library allocated {per_conn} bytes per idle connection"
        );
        drop(clients);
        shutdown.shutdown();
        join.join().unwrap().unwrap();
    }
}
