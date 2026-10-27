//! Windows I/O-completion-port reactor: AcceptEx/ConnectEx/WSARecv/WSASend.
//!
//! The completion backend posts at most one receive and one send per connection.
//! A completion is identified by the address of its `OVERLAPPED`, whose owning
//! box stays in [`IocpReactor::posted`] until the matching completion is
//! dequeued; the generation-checked [`ConnId`] carried by every operation is
//! then matched against the slab before anything is touched. Closing cancels
//! outstanding operations with `CancelIoEx` and frees the slot only after all of
//! them have completed.
//!
//! Unlike the Unix readiness loop there is no event to re-arm: an operation is
//! either posted or not. Receive completions that arrive while a send is in
//! flight are held in [`ConnOps::recv_ready`] so the shared handler rules only
//! ever run with no posted send. That keeps the core's immediate close on a
//! handler error safe: it can never run while an operation still targets the
//! connection's socket.

#![cfg(windows)]

use std::collections::HashMap;
use std::io;
use std::mem::size_of;
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::os::windows::io::{AsRawSocket, FromRawSocket, RawSocket};
use std::sync::Arc;
use std::time::{Duration, Instant};

use windows_sys::Win32::Networking::WinSock::{
    ADDRESS_FAMILY, AF_INET, AF_INET6, AcceptEx, INVALID_SOCKET, SOCKADDR_STORAGE, SOCKET,
    SOCKET_ERROR, WSA_IO_PENDING, WSABUF, WSARecv, WSASend, listen,
};
use windows_sys::Win32::System::IO::{OVERLAPPED, OVERLAPPED_ENTRY};

use crate::rt::block::BLOCK_SIZE;
use crate::rt::conn::Conn;
#[cfg(test)]
use crate::rt::handler::EchoHandler;
use crate::rt::handler::Handler;
use crate::rt::poller::iocp::{
    CONN_KEY, Iocp, LISTEN_KEY, MAX_COMPLETIONS, SockAddrBuf, WAKE_KEY, bind_wildcard, cancel,
    close_socket, create_socket, last_wsa_error, load_connect_ex, overlapped_result,
    set_accept_context, set_connect_context,
};
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

/// Pending `AcceptEx` operations kept armed on the listener.
const ACCEPT_POOL: usize = 16;

/// Address bytes reserved for one `AcceptEx` endpoint.
const ACCEPT_ADDR_LEN: u32 = size_of::<SOCKADDR_STORAGE>() as u32 + 16;

/// `AcceptEx` output buffer: local plus remote address.
const ACCEPT_BUF_LEN: usize = 2 * ACCEPT_ADDR_LEN as usize;

/// Blocks gathered into one `WSASend`.
const MAX_IOVECS: usize = 64;

/// Upper bound on one completion wait, short enough for shutdown and timers.
const MAX_WAIT: Duration = Duration::from_secs(1);

/// Default listen backlog, matching the standard library's bind.
const DEFAULT_BACKLOG: usize = 1_024;

/// How long dropping the reactor waits for canceled operations to complete.
const DROP_WAIT: Duration = Duration::from_secs(2);

/// The completion port does not touch memory after a completion is dequeued, so
/// moving an operation to the reactor thread transfers exclusive access to it.
/// The kernel writes to `overlapped` from its own context while the operation is
/// posted, which is exactly the ownership contract this type relies on.
unsafe impl Send for RecvOp {}
/// See [`RecvOp`].
unsafe impl Send for SendOp {}
/// See [`RecvOp`].
unsafe impl Send for AcceptOp {}
/// See [`RecvOp`].
unsafe impl Send for ConnectOp {}

/// One posted `WSARecv`.
struct RecvOp {
    socket: SOCKET,
    overlapped: OVERLAPPED,
    buf: Box<[u8]>,
}

impl RecvOp {
    fn new() -> Self {
        Self {
            socket: 0,
            overlapped: OVERLAPPED::default(),
            buf: vec![0u8; BLOCK_SIZE].into_boxed_slice(),
        }
    }

    fn key(&self) -> usize {
        std::ptr::from_ref(&self.overlapped) as usize
    }
}

/// One posted `WSASend`.
///
/// `bufs` points into the connection's queued blocks. The queue is not mutated
/// while the send is posted, so those ranges stay valid until completion.
struct SendOp {
    socket: SOCKET,
    overlapped: OVERLAPPED,
    bufs: Vec<WSABUF>,
}

impl SendOp {
    fn new() -> Self {
        Self {
            socket: 0,
            overlapped: OVERLAPPED::default(),
            bufs: Vec::new(),
        }
    }

    fn key(&self) -> usize {
        std::ptr::from_ref(&self.overlapped) as usize
    }
}

/// One posted `AcceptEx`.
struct AcceptOp {
    listener: SOCKET,
    socket: SOCKET,
    overlapped: OVERLAPPED,
    buf: Box<[u8]>,
}

impl AcceptOp {
    fn key(&self) -> usize {
        std::ptr::from_ref(&self.overlapped) as usize
    }

    /// Relinquishes the accepted socket so wrapping it does not double-close.
    fn take_socket(&mut self) -> SOCKET {
        std::mem::replace(&mut self.socket, INVALID_SOCKET)
    }
}

impl Drop for AcceptOp {
    fn drop(&mut self) {
        close_socket(self.socket);
    }
}

/// One posted `ConnectEx`.
struct ConnectOp {
    socket: SOCKET,
    overlapped: OVERLAPPED,
}

impl ConnectOp {
    fn key(&self) -> usize {
        std::ptr::from_ref(&self.overlapped) as usize
    }

    fn take_socket(&mut self) -> SOCKET {
        std::mem::replace(&mut self.socket, INVALID_SOCKET)
    }
}

impl Drop for ConnectOp {
    fn drop(&mut self) {
        close_socket(self.socket);
    }
}

/// An operation whose `OVERLAPPED` is currently owned by the completion port.
enum Posted {
    Accept(Box<AcceptOp>),
    Recv {
        id: ConnId,
        op: Box<RecvOp>,
    },
    Send {
        id: ConnId,
        op: Box<SendOp>,
    },
    /// A pre-loop connect whose result is picked up by `connect_result`.
    Connect {
        op: Box<ConnectOp>,
    },
    /// A test-client connect; its acknowledgement receives the result.
    #[cfg(test)]
    TestConnect {
        op: Box<ConnectOp>,
        ack: std::sync::mpsc::Sender<Reply>,
    },
}

impl Posted {
    fn overlapped(&self) -> *const OVERLAPPED {
        match self {
            Self::Accept(op) => std::ptr::from_ref(&op.overlapped),
            Self::Recv { op, .. } => std::ptr::from_ref(&op.overlapped),
            Self::Send { op, .. } => std::ptr::from_ref(&op.overlapped),
            Self::Connect { op } => std::ptr::from_ref(&op.overlapped),
            #[cfg(test)]
            Self::TestConnect { op, .. } => std::ptr::from_ref(&op.overlapped),
        }
    }

    /// The handle `CancelIoEx`/`GetOverlappedResult` must be called on.
    fn socket(&self) -> SOCKET {
        match self {
            // AcceptEx is issued on the listener, not the not-yet-accepted socket.
            Self::Accept(op) => op.listener,
            Self::Recv { op, .. } => op.socket,
            Self::Send { op, .. } => op.socket,
            Self::Connect { op } => op.socket,
            #[cfg(test)]
            Self::TestConnect { op, .. } => op.socket,
        }
    }

    fn id(&self) -> Option<ConnId> {
        match self {
            Self::Accept(_) | Self::Connect { .. } => None,
            Self::Recv { id, .. } | Self::Send { id, .. } => Some(*id),
            #[cfg(test)]
            Self::TestConnect { .. } => None,
        }
    }
}

/// Per-connection completion state.
///
/// `recv` and `send` hold allocated operations only while they are *not*
/// posted; the `posted` map owns an operation while the port can complete it.
struct ConnOps {
    recv: Option<Box<RecvOp>>,
    recv_ready: Option<usize>,
    eof_pending: bool,
    send: Option<Box<SendOp>>,
    recv_posted: bool,
    send_posted: bool,
    closing: bool,
}

impl ConnOps {
    fn new() -> Self {
        Self {
            recv: None,
            recv_ready: None,
            eof_pending: false,
            send: None,
            recv_posted: false,
            send_posted: false,
            closing: false,
        }
    }

    fn pending(&self) -> usize {
        usize::from(self.recv_posted as u8) + usize::from(self.send_posted as u8)
    }
}

/// Completion reactor for Windows.
pub(crate) struct IocpReactor<H: Handler> {
    core: ReactorCore<H>,
    /// Created lazily so `new` cannot fail; `listen`, `connect` and `run_until`
    /// all establish it before posting anything.
    poller: Option<Iocp>,
    listener: Option<TcpListener>,
    listener_family: ADDRESS_FAMILY,
    conns: HashMap<ConnId, ConnOps>,
    posted: HashMap<usize, Posted>,
    /// Result of the single synchronous `connect` operation currently waiting.
    connect_result: Option<(usize, io::Result<SOCKET>)>,
    /// Set when the wake sentinel is dequeued.
    stop: bool,
    /// Test-only command channel installed by the harness.
    #[cfg(test)]
    control: Option<Arc<ControlChannel>>,
}

impl<H: Handler> IocpReactor<H> {
    /// Builds a reactor with an empty slab, pool, timer heap and completion port.
    pub(crate) fn new(handler: H) -> Self {
        Self {
            core: ReactorCore::new(handler),
            poller: None,
            listener: None,
            listener_family: AF_INET,
            conns: HashMap::new(),
            posted: HashMap::new(),
            connect_result: None,
            stop: false,
            #[cfg(test)]
            control: None,
        }
    }

    /// Binds a listener to `addr` and posts a bounded `AcceptEx` pool.
    pub(crate) fn listen(&mut self, addr: &str) -> io::Result<()> {
        self.listen_with_backlog(addr, DEFAULT_BACKLOG)
    }

    /// Binds a listener with an explicit kernel backlog.
    ///
    /// The standard library's bind fixes the backlog, so the 10k-idle harness
    /// re-issues `listen` on the bound socket. Windows only updates the queue
    /// limit for a socket that is already listening.
    pub(crate) fn listen_with_backlog(&mut self, addr: &str, backlog: usize) -> io::Result<()> {
        if self.listener.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "reactor already has a listener",
            ));
        }

        let listener = TcpListener::bind(addr)?;
        let local = listener.local_addr()?;
        let family = if local.is_ipv4() { AF_INET } else { AF_INET6 };
        let raw = listener.as_raw_socket() as SOCKET;
        let backlog = i32::try_from(backlog).unwrap_or(i32::MAX);
        // SAFETY: `raw` is a live listening socket; `listen` only updates the
        // backlog bound and does not read or write memory.
        if unsafe { listen(raw, backlog) } == SOCKET_ERROR {
            return Err(last_wsa_error());
        }

        self.poller_mut()?.associate(raw, LISTEN_KEY)?;
        self.listener = Some(listener);
        self.listener_family = family;

        // A failure leaves the listener armed for teardown; `Drop` cancels any
        // accepts that were already posted.
        for _ in 0..ACCEPT_POOL {
            self.post_accept()?;
        }
        Ok(())
    }

    /// Opens an outbound connection with `ConnectEx` and reports it to the
    /// handler.
    ///
    /// This mirrors the Unix reactor's blocking `connect`: it waits for the
    /// completion before returning the generation-checked id.
    pub(crate) fn connect(&mut self, addr: &str) -> io::Result<ConnId> {
        let peer = addr.to_socket_addrs()?.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "address resolved to nothing")
        })?;
        let family = if peer.is_ipv4() { AF_INET } else { AF_INET6 };
        let socket = create_socket(family)?;
        let socket = self.begin_connect(socket, peer)?;
        self.finish_connect(socket)
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

    /// Runs the completion loop until the wake sentinel is dequeued.
    pub(crate) fn run_until(&mut self, _shutdown: &Wake) -> io::Result<()> {
        if self.poller.is_none() {
            self.poller = Some(Iocp::new()?);
        }

        let mut entries = [OVERLAPPED_ENTRY::default(); MAX_COMPLETIONS];
        while !self.stop {
            #[cfg(test)]
            self.apply_control_commands();
            let timeout = self.poll_timeout();
            let count = self
                .poller
                .as_ref()
                .expect("poller was established")
                .wait(Some(timeout), &mut entries)?;

            if count > 0 {
                for entry in entries.iter().take(count) {
                    self.handle_entry(entry)?;
                    if self.stop {
                        break;
                    }
                }
            }
            self.fire_timers();
        }
        Ok(())
    }

    /// Returns the poller, creating it on first use.
    fn poller_mut(&mut self) -> io::Result<&mut Iocp> {
        if self.poller.is_none() {
            self.poller = Some(Iocp::new()?);
        }
        Ok(self.poller.as_mut().expect("poller was just initialized"))
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
                    }
                    Reply::Done
                }
                CommandKind::Resume => {
                    for id in control.ids() {
                        if let Some(conn) = self.core.conn_mut(id) {
                            conn.resume_read();
                        }
                        self.resume_io(id);
                    }
                    Reply::Done
                }
                CommandKind::Close(id) => {
                    self.begin_close(id);
                    Reply::Done
                }
                CommandKind::PendingOps(id) => {
                    Reply::Count(self.conns.get(&id).map_or(0, ConnOps::pending))
                }
                CommandKind::CloseNoWait(id) => {
                    self.begin_close(id);
                    Reply::Done
                }
                CommandKind::MarkReusable(id) => {
                    self.mark_connection_reusable(id);
                    Reply::Done
                }
                CommandKind::DeliverStale(id) => {
                    self.deliver_stale(id);
                    Reply::Done
                }
                CommandKind::Connect(addr) => {
                    // The ConnectEx completion replies through the
                    // acknowledgement channel once it is dequeued.
                    self.begin_connect_async(addr, command.ack);
                    continue;
                }
            };
            let _ = command.ack.send(reply);
        }
    }

    /// Re-arms reading after an explicit resume, first delivering a read that
    /// completed while the connection was paused.
    #[cfg(test)]
    fn resume_io(&mut self, id: ConnId) {
        let Some(ops) = self.conns.get(&id) else {
            return;
        };
        let has_ready_read = ops.recv_ready.is_some();
        let send_posted = ops.send_posted;
        if has_ready_read && !send_posted {
            self.process_ready_read(id);
        } else {
            self.post_recv(id);
        }
    }

    /// Test-only: free the slab slot while canceled completions are pending.
    #[cfg(test)]
    fn mark_connection_reusable(&mut self, id: ConnId) {
        if self.conns.get(&id).is_some_and(|ops| ops.send_posted) {
            // Freeing the connection would invalidate the blocks a posted send
            // still points at; refuse rather than risk use-after-free.
            debug_assert!(false, "cannot free a slot whose send is posted");
            return;
        }
        self.conns.remove(&id);
        self.core.close(id, "test-reuse");
    }

    /// Test-only: wait for the real canceled completions for `id` and route
    /// them through the normal dispatch path. The generation check must discard
    /// them.
    ///
    /// The operation box may only be freed once the kernel has dequeued its
    /// completion, so this cannot synthesize a completion; `close_no_wait`
    /// already called `CancelIoEx`, and the aborted completion follows.
    #[cfg(test)]
    fn deliver_stale(&mut self, id: ConnId) {
        let mut entries = [OVERLAPPED_ENTRY::default(); MAX_COMPLETIONS];
        let deadline = Instant::now() + DROP_WAIT;
        while self.posted.values().any(|posted| posted.id() == Some(id))
            && Instant::now() < deadline
            && !self.stop
        {
            let count = match self.poller.as_ref() {
                Some(poller) => match poller.wait(Some(Duration::from_millis(20)), &mut entries) {
                    Ok(count) => count,
                    Err(_) => return,
                },
                None => return,
            };
            for entry in entries.iter().take(count) {
                if self.handle_entry(entry).is_err() {
                    return;
                }
            }
        }
    }

    /// Applies one dequeued completion packet.
    fn handle_entry(&mut self, entry: &OVERLAPPED_ENTRY) -> io::Result<()> {
        if entry.lpCompletionKey == WAKE_KEY {
            #[cfg(test)]
            if let Some(control) = self.control.clone() {
                if control.stop() {
                    self.stop = true;
                } else {
                    self.apply_control_commands();
                }
                return Ok(());
            }
            self.stop = true;
            return Ok(());
        }
        if entry.lpOverlapped.is_null() {
            return Ok(());
        }

        let key = entry.lpOverlapped as usize;
        let Some(posted) = self.posted.remove(&key) else {
            // A canceled operation whose completion was already observed, or a
            // packet from another reactor sharing the process. Nothing is owned
            // here, so there is nothing to release.
            return Ok(());
        };

        match posted {
            Posted::Accept(op) => self.handle_accept_completion(op),
            Posted::Recv { id, op } => {
                self.handle_recv_completion(id, op);
                Ok(())
            }
            Posted::Send { id, op } => {
                self.handle_send_completion(id, op);
                Ok(())
            }
            Posted::Connect { mut op } => {
                let key = op.key();
                let socket = op.take_socket();
                let result = match overlapped_result(socket, &op.overlapped) {
                    Ok(_) => Ok(socket),
                    Err(err) => {
                        close_socket(socket);
                        Err(err)
                    }
                };
                self.connect_result = Some((key, result));
                Ok(())
            }
            #[cfg(test)]
            Posted::TestConnect { mut op, ack } => {
                let socket = op.take_socket();
                let reply = match overlapped_result(socket, &op.overlapped) {
                    Ok(_) => self.finish_connect(socket),
                    Err(err) => {
                        close_socket(socket);
                        Err(err)
                    }
                };
                let _ = ack.send(Reply::Connect(reply));
                Ok(())
            }
        }
    }

    /// Posts a new `AcceptEx` while the pool has room.
    fn post_accept(&mut self) -> io::Result<()> {
        let Some(listener) = self.listener.as_ref() else {
            return Ok(());
        };
        let listener_socket = listener.as_raw_socket() as SOCKET;
        let family = self.listener_family;
        let socket = create_socket(family)?;

        let mut op = Box::new(AcceptOp {
            listener: listener_socket,
            socket,
            overlapped: OVERLAPPED::default(),
            buf: vec![0u8; ACCEPT_BUF_LEN].into_boxed_slice(),
        });

        // SAFETY: the listener and accept socket are live, the output buffer is
        // valid for the two address lengths, and the overlapped structure lives
        // in `op` until its completion is dequeued.
        let ok = unsafe {
            AcceptEx(
                listener_socket as SOCKET,
                socket as SOCKET,
                op.buf.as_mut_ptr().cast(),
                0,
                ACCEPT_ADDR_LEN,
                ACCEPT_ADDR_LEN,
                std::ptr::null_mut(),
                &mut op.overlapped,
            )
        };
        if ok == 0 {
            let err = last_wsa_error();
            if err.raw_os_error() != Some(WSA_IO_PENDING) {
                let socket = op.take_socket();
                close_socket(socket);
                return Err(err);
            }
        }

        let key = op.key();
        self.posted.insert(key, Posted::Accept(op));
        Ok(())
    }

    /// Handles one `AcceptEx` completion and immediately re-arms the pool.
    fn handle_accept_completion(&mut self, mut op: Box<AcceptOp>) -> io::Result<()> {
        if overlapped_result(op.listener, &op.overlapped).is_ok() {
            // A per-connection accept failure is not fatal for the listener: the
            // accepted socket is closed and another accept takes its place.
            let _ = self.adopt_accept(&mut op);
        }

        if self.stop || self.listener.is_none() {
            return Ok(());
        }
        self.post_accept()
    }

    /// Turns a successfully accepted socket into a live connection.
    fn adopt_accept(&mut self, op: &mut AcceptOp) -> io::Result<()> {
        let accepted = op.take_socket();

        if let Err(err) = set_accept_context(accepted as SOCKET, op.listener as SOCKET) {
            close_socket(accepted);
            return Err(err);
        }
        if let Err(err) = self.poller_mut()?.associate(accepted, CONN_KEY) {
            close_socket(accepted);
            return Err(err);
        }

        // SAFETY: `accepted` is a connected overlapped socket whose ownership is
        // transferred to the stream; it is not used through the raw handle after
        // this point.
        let stream = unsafe { TcpStream::from_raw_socket(accepted as RawSocket) };
        let socket = OwnedSocket::new(stream)?;
        let conn = Conn::new(socket, Arc::clone(self.core.pool()), WRITE_CAP)?;
        let id = self.core.insert(conn);
        self.conns.insert(id, ConnOps::new());

        if self.core.on_accept(id).is_err() {
            // The core already closed the connection; no operation was posted.
            self.conns.remove(&id);
            return Ok(());
        }

        self.post_send(id);
        self.post_recv(id);
        Ok(())
    }

    /// Starts a `ConnectEx` and blocks until its completion is dequeued.
    ///
    /// On success the connected socket is returned. Setup failures close the
    /// socket here; once the operation is posted, its [`ConnectOp`] owns the
    /// socket until the completion is handled.
    fn begin_connect(&mut self, socket: SOCKET, peer: SocketAddr) -> io::Result<SOCKET> {
        let family = if peer.is_ipv4() { AF_INET } else { AF_INET6 };

        let setup = (|| -> io::Result<_> {
            self.poller_mut()?.associate(socket, CONN_KEY)?;
            bind_wildcard(socket as SOCKET, family)?;
            load_connect_ex(socket as SOCKET)
        })();
        let connect_ex = match setup {
            Ok(connect_ex) => connect_ex,
            Err(err) => {
                close_socket(socket);
                return Err(err);
            }
        };

        let address = SockAddrBuf::from_socket_addr(peer);
        let mut op = Box::new(ConnectOp {
            socket,
            overlapped: OVERLAPPED::default(),
        });
        // SAFETY: the socket is bound and associated, the sockaddr is valid for
        // its reported length, and the overlapped structure stays alive in
        // `op` until the completion is dequeued.
        let ok = unsafe {
            connect_ex(
                socket as SOCKET,
                address.as_ptr(),
                address.len(),
                std::ptr::null(),
                0,
                std::ptr::null_mut(),
                &mut op.overlapped,
            )
        };
        if ok == 0 {
            let err = last_wsa_error();
            if err.raw_os_error() != Some(WSA_IO_PENDING) {
                let socket = op.take_socket();
                close_socket(socket);
                return Err(err);
            }
        }

        let key = op.key();
        self.posted.insert(key, Posted::Connect { op });

        let mut entries = [OVERLAPPED_ENTRY::default(); MAX_COMPLETIONS];
        let mut cancelled = false;
        loop {
            if let Some((done, result)) = self.connect_result.take() {
                if done == key {
                    if self.stop {
                        if let Ok(socket) = result {
                            close_socket(socket);
                        }
                        return Err(io::Error::new(
                            io::ErrorKind::ConnectionAborted,
                            "connect canceled by shutdown",
                        ));
                    }
                    return result;
                }
                if let Ok(socket) = result {
                    close_socket(socket);
                }
            }

            if self.stop && !cancelled {
                if let Some(Posted::Connect { op }) = self.posted.get(&key) {
                    let _ = cancel(op.socket, &op.overlapped);
                }
                cancelled = true;
            }

            let count = self
                .poller
                .as_ref()
                .expect("connect established the poller")
                .wait(Some(MAX_WAIT), &mut entries)?;
            for entry in entries.iter().take(count) {
                self.handle_entry(entry)?;
            }
        }
    }

    /// Wraps a completed `ConnectEx` socket as a live connection.
    fn finish_connect(&mut self, socket: SOCKET) -> io::Result<ConnId> {
        if let Err(err) = set_connect_context(socket as SOCKET) {
            close_socket(socket);
            return Err(err);
        }

        // SAFETY: the socket finished connecting and is not used through the raw
        // handle after this point; the stream takes ownership.
        let stream = unsafe { TcpStream::from_raw_socket(socket as RawSocket) };
        let socket = OwnedSocket::new(stream)?;
        let conn = Conn::new(socket, Arc::clone(self.core.pool()), WRITE_CAP)?;
        let id = self.core.insert(conn);
        self.conns.insert(id, ConnOps::new());

        if let Err(err) = self.core.on_connect(id) {
            // The core already closed the connection; no operation was posted.
            self.conns.remove(&id);
            return Err(err);
        }

        self.post_send(id);
        self.post_recv(id);
        Ok(id)
    }

    /// Starts a `ConnectEx` for the test client and returns immediately.
    ///
    /// The completion is dequeued by the normal completion loop, which adopts
    /// the socket through [`Self::finish_connect`] and replies on `ack`. Setup
    /// failures are reported here; once the operation is posted its
    /// [`ConnectOp`] owns the socket.
    #[cfg(test)]
    fn begin_connect_async(&mut self, peer: SocketAddr, ack: std::sync::mpsc::Sender<Reply>) {
        let family = if peer.is_ipv4() { AF_INET } else { AF_INET6 };
        let socket = match create_socket(family) {
            Ok(socket) => socket,
            Err(err) => {
                let _ = ack.send(Reply::Connect(Err(err)));
                return;
            }
        };

        let setup = (|| -> io::Result<_> {
            self.poller_mut()?.associate(socket, CONN_KEY)?;
            bind_wildcard(socket, family)?;
            load_connect_ex(socket)
        })();
        let connect_ex = match setup {
            Ok(connect_ex) => connect_ex,
            Err(err) => {
                close_socket(socket);
                let _ = ack.send(Reply::Connect(Err(err)));
                return;
            }
        };

        let address = SockAddrBuf::from_socket_addr(peer);
        let mut op = Box::new(ConnectOp {
            socket,
            overlapped: OVERLAPPED::default(),
        });
        // SAFETY: the socket is bound and associated, the sockaddr is valid for
        // its reported length, and the overlapped structure stays alive in
        // `op` until the completion is dequeued.
        let ok = unsafe {
            connect_ex(
                socket as SOCKET,
                address.as_ptr(),
                address.len(),
                std::ptr::null(),
                0,
                std::ptr::null_mut(),
                &mut op.overlapped,
            )
        };
        if ok == 0 {
            let err = last_wsa_error();
            if err.raw_os_error() != Some(WSA_IO_PENDING) {
                let socket = op.take_socket();
                close_socket(socket);
                let _ = ack.send(Reply::Connect(Err(err)));
                return;
            }
        }

        let key = op.key();
        self.posted.insert(key, Posted::TestConnect { op, ack });
    }

    /// Posts one `WSARecv` when reading is allowed.
    fn post_recv(&mut self, id: ConnId) {
        let Some(conn) = self.core.conn(id) else {
            return;
        };
        if conn.is_paused() {
            return;
        }
        let socket = conn.socket().as_raw_socket() as SOCKET;

        let Some(ops) = self.conns.get_mut(&id) else {
            return;
        };
        if ops.recv_posted || ops.closing || ops.recv_ready.is_some() || ops.eof_pending {
            return;
        }

        let mut op = ops.recv.take().unwrap_or_else(|| Box::new(RecvOp::new()));
        op.socket = socket;
        op.overlapped = OVERLAPPED::default();
        let wsabuf = WSABUF {
            len: BLOCK_SIZE as u32,
            buf: op.buf.as_mut_ptr(),
        };
        let mut flags = 0u32;

        // SAFETY: the socket is live and associated with the completion port;
        // `wsabuf` points into `op.buf`, which stays alive in `posted` until the
        // completion is dequeued. The overlapped structure is owned by `op`.
        let rc = unsafe {
            WSARecv(
                socket as SOCKET,
                &wsabuf,
                1,
                std::ptr::null_mut(),
                &mut flags,
                &mut op.overlapped,
                None,
            )
        };

        if rc == SOCKET_ERROR {
            let err = last_wsa_error();
            if err.raw_os_error() != Some(WSA_IO_PENDING) {
                ops.recv = Some(op);
                self.begin_close(id);
                return;
            }
        }

        ops.recv_posted = true;
        let key = op.key();
        self.posted.insert(key, Posted::Recv { id, op });
    }

    /// Posts one `WSASend` for the front of the outbound queue.
    fn post_send(&mut self, id: ConnId) {
        let Some(conn) = self.core.conn(id) else {
            return;
        };
        if !conn.has_pending_write() {
            return;
        }
        let socket = conn.socket().as_raw_socket() as SOCKET;

        let Some(ops) = self.conns.get_mut(&id) else {
            return;
        };
        if ops.send_posted || ops.closing {
            return;
        }

        let mut op = ops.send.take().unwrap_or_else(|| Box::new(SendOp::new()));
        op.socket = socket;
        op.overlapped = OVERLAPPED::default();
        op.bufs.clear();
        for block in conn.pending_blocks().iter().take(MAX_IOVECS) {
            op.bufs.push(WSABUF {
                len: block.len() as u32,
                buf: block.as_slice().as_ptr().cast_mut(),
            });
        }

        if op.bufs.is_empty() {
            ops.send = Some(op);
            return;
        }

        // SAFETY: `op.bufs` points into blocks owned by the connection, which
        // are not mutated until this operation completes; the socket is live and
        // the overlapped structure is owned by `op`.
        let rc = unsafe {
            WSASend(
                socket as SOCKET,
                op.bufs.as_ptr(),
                op.bufs.len() as u32,
                std::ptr::null_mut(),
                0,
                &mut op.overlapped,
                None,
            )
        };

        if rc == SOCKET_ERROR {
            let err = last_wsa_error();
            if err.raw_os_error() != Some(WSA_IO_PENDING) {
                op.bufs.clear();
                ops.send = Some(op);
                self.begin_close(id);
                return;
            }
        }

        ops.send_posted = true;
        let key = op.key();
        self.posted.insert(key, Posted::Send { id, op });
    }

    /// Applies a receive completion.
    fn handle_recv_completion(&mut self, id: ConnId, op: Box<RecvOp>) {
        let Some(ops) = self.conns.get_mut(&id) else {
            return;
        };
        ops.recv_posted = false;
        if ops.closing {
            let drained = ops.pending() == 0;
            if drained {
                self.finalize_close(id);
            }
            return;
        }
        if self.core.conn(id).is_none() {
            self.conns.remove(&id);
            return;
        }

        let bytes = match overlapped_result(op.socket, &op.overlapped) {
            Ok(bytes) => bytes as usize,
            Err(_) => {
                self.begin_close(id);
                return;
            }
        };

        if bytes == 0 {
            if self.conns.get(&id).is_some_and(|ops| ops.send_posted) {
                if let Some(ops) = self.conns.get_mut(&id) {
                    ops.eof_pending = true;
                }
                return;
            }
            self.core.on_eof(id);
            self.flush_deferred_eof(id);
            return;
        }

        // Reading is paused (explicitly or by backpressure), or a send is in
        // flight: hold the completed bytes instead of delivering them. A pause
        // command synchronous with this reactor must not race a new read.
        let paused = self.core.conn(id).is_some_and(|conn| conn.is_paused());
        if paused || self.conns.get(&id).is_some_and(|ops| ops.send_posted) {
            if let Some(ops) = self.conns.get_mut(&id) {
                ops.recv = Some(op);
                ops.recv_ready = Some(bytes);
            }
            return;
        }

        self.deliver_read(id, op, bytes);
    }

    /// Hands a completed receive buffer to the handler, then re-arms both sides.
    fn deliver_read(&mut self, id: ConnId, op: Box<RecvOp>, len: usize) {
        if self.core.conn(id).is_none() {
            return;
        }
        #[cfg(test)]
        self.core.note_read(len);
        if self.core.on_read_ready(id, &op.buf[..len]).is_err() {
            // A handler error is terminal; the core closed the connection. No
            // operation is posted on either side at this point.
            self.conns.remove(&id);
            return;
        }

        let Some(ops) = self.conns.get_mut(&id) else {
            return;
        };
        ops.recv = Some(op);
        self.post_send(id);
        self.post_recv(id);
    }

    /// Completes an EOF that arrived while a deferred flush was outstanding.
    ///
    /// A carried handler chunk counts as outbound work: retry it into the queue
    /// (the core sets `close_after_flush` instead of closing) and post its
    /// send. The close then happens when that send completes.
    fn flush_deferred_eof(&mut self, id: ConnId) {
        if self.core.conn(id).is_none() {
            self.conns.remove(&id);
            return;
        }
        if self.core.conn(id).is_some_and(|conn| conn.has_carry()) {
            let _ = self.core.on_write_ready(id);
        }
        if self.core.conn(id).is_none() {
            self.conns.remove(&id);
            return;
        }
        if self
            .core
            .conn(id)
            .is_some_and(|conn| conn.has_pending_write())
        {
            self.post_send(id);
        }
    }

    /// Processes a read that was held while a send was in flight.
    fn process_ready_read(&mut self, id: ConnId) {
        let taken = self.conns.get_mut(&id).and_then(|ops| {
            let len = ops.recv_ready.take()?;
            let op = ops.recv.take()?;
            Some((op, len))
        });
        let Some((op, len)) = taken else {
            return;
        };
        self.deliver_read(id, op, len);
    }

    /// Applies a send completion.
    fn handle_send_completion(&mut self, id: ConnId, mut op: Box<SendOp>) {
        op.bufs.clear();

        let Some(ops) = self.conns.get_mut(&id) else {
            return;
        };
        ops.send_posted = false;
        if ops.closing {
            let drained = ops.pending() == 0;
            if drained {
                self.finalize_close(id);
            }
            return;
        }
        if self.core.conn(id).is_none() {
            self.conns.remove(&id);
            return;
        }

        let bytes = match overlapped_result(op.socket, &op.overlapped) {
            Ok(bytes) => bytes as usize,
            Err(_) => {
                self.begin_close(id);
                return;
            }
        };
        if bytes == 0 {
            // No progress is terminal rather than a spin on a ready socket.
            self.begin_close(id);
            return;
        }

        let drained = match self.core.conn_mut(id) {
            Some(conn) => {
                conn.consume_written(bytes);
                !conn.has_pending_write()
            }
            None => {
                self.conns.remove(&id);
                return;
            }
        };

        if let Some(ops) = self.conns.get_mut(&id) {
            ops.send = Some(op);
        }

        if !drained {
            self.post_send(id);
            return;
        }

        if self.core.on_write_ready(id).is_err() {
            self.conns.remove(&id);
            return;
        }
        if self.core.conn(id).is_none() {
            self.conns.remove(&id);
            return;
        }
        // The handler may have retried a carried chunk into the empty queue.
        if self
            .core
            .conn(id)
            .is_some_and(|conn| conn.has_pending_write())
        {
            self.post_send(id);
        }

        if self.conns.get(&id).is_some_and(|ops| ops.eof_pending) {
            if let Some(ops) = self.conns.get_mut(&id) {
                ops.eof_pending = false;
            }
            self.core.on_eof(id);
            self.flush_deferred_eof(id);
            return;
        }

        if self
            .conns
            .get(&id)
            .is_some_and(|ops| ops.recv_ready.is_some())
        {
            self.process_ready_read(id);
        } else {
            self.post_recv(id);
        }
    }

    /// Requests close, cancels posted operations and frees the slot after their
    /// completions.
    fn begin_close(&mut self, id: ConnId) {
        let Some(ops) = self.conns.get_mut(&id) else {
            self.core.close(id, "not-live");
            return;
        };
        if ops.closing {
            return;
        }
        ops.closing = true;
        ops.recv = None;
        ops.recv_ready = None;
        ops.eof_pending = false;
        if !ops.send_posted {
            ops.send = None;
        }
        let pending = ops.pending();

        let keys: Vec<usize> = self
            .posted
            .iter()
            .filter(|(_, posted)| posted.id() == Some(id))
            .map(|(key, _)| *key)
            .collect();
        for key in keys {
            if let Some(posted) = self.posted.get(&key) {
                // A failed cancel is not actionable: `ERROR_NOT_FOUND` means the
                // completion is already queued, and every other error leaves the
                // operation owned by `posted` until its completion arrives.
                let _ = cancel(posted.socket(), posted.overlapped());
            }
        }

        if pending == 0 {
            self.finalize_close(id);
        }
    }

    /// Drops the connection state and reports the close through the core.
    fn finalize_close(&mut self, id: ConnId) {
        debug_assert!(
            self.conns.get(&id).is_none_or(|ops| ops.pending() == 0),
            "freeing a connection with posted operations"
        );
        self.conns.remove(&id);
        self.core.close(id, "reactor-close");
    }
}

#[cfg(test)]
impl<H: Handler + Send + 'static> IocpReactor<H> {
    /// Binds `addr`, then moves the reactor onto a background thread.
    ///
    /// The returned [`Shutdown`] posts the wake sentinel; the join handle carries
    /// the loop's result.
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
        let port = self
            .poller
            .as_ref()
            .expect("listen created the completion port")
            .raw_handle();
        let wake = Arc::new(Wake::new(port)?);
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

#[cfg(test)]
impl<H: Handler + SpawnClient + Send + 'static> IocpReactor<H> {
    /// Spawns a listenerless reactor for the outbound-connect tests.
    ///
    /// The completion port is created before the wake handle because a Windows
    /// wake is a sentinel packet posted to that port.
    pub(crate) fn spawn_client_for_test() -> (
        TestClientHandle,
        Shutdown,
        std::thread::JoinHandle<io::Result<()>>,
    ) {
        let (handler, state) = H::new_client();
        let mut reactor = Self::new(handler);
        let port = reactor
            .poller_mut()
            .expect("create the completion port")
            .raw_handle();
        let wake = Arc::new(Wake::new(port).expect("create the reactor wake"));
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
impl IocpReactor<EchoHandler> {
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

impl<H: Handler> Drop for IocpReactor<H> {
    fn drop(&mut self) {
        // An abandoned connect result still owns its connected socket.
        if let Some((_, Ok(socket))) = self.connect_result.take() {
            close_socket(socket);
        }

        // Cancel every operation the port could still complete and wait for its
        // completion before the operation boxes are freed. This is the ordering
        // the design requires for Windows: cancel, wait, then close.
        let pending: Vec<(SOCKET, *const OVERLAPPED)> = self
            .posted
            .values()
            .map(|posted| (posted.socket(), posted.overlapped()))
            .collect();
        for (socket, overlapped) in pending {
            let _ = cancel(socket, overlapped);
        }

        let mut entries = [OVERLAPPED_ENTRY::default(); MAX_COMPLETIONS];
        let deadline = Instant::now() + DROP_WAIT;
        while !self.posted.is_empty() && Instant::now() < deadline {
            let count = match self.poller.as_ref() {
                Some(poller) => match poller.wait(Some(Duration::from_millis(50)), &mut entries) {
                    Ok(count) => count,
                    Err(_) => break,
                },
                None => break,
            };
            for entry in entries.iter().take(count) {
                if entry.lpOverlapped.is_null() {
                    continue;
                }
                self.posted.remove(&(entry.lpOverlapped as usize));
            }
        }

        if !self.posted.is_empty() {
            // The kernel may still write to a posted `OVERLAPPED`; leaking the
            // boxes is the only memory-safe fallback if it never completes.
            std::mem::forget(std::mem::take(&mut self.posted));
        }
        self.conns.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::IocpReactor;
    use crate::rt::handler::EchoHandler;
    use crate::rt::test_support::{
        ConnectHandler, spawn_echo, spawn_echo_on, spawn_echo_with_control,
    };
    use std::io::{self, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::Duration;

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

    #[test]
    fn iocp_serves_two_clients_from_the_accept_pool() {
        let (addr, shutdown, join) = spawn_echo();

        let mut first = TcpStream::connect(addr).unwrap();
        let mut second = TcpStream::connect(addr).unwrap();
        first.write_all(b"one").unwrap();
        second.write_all(b"two").unwrap();

        let mut first_buf = [0u8; 3];
        let mut second_buf = [0u8; 3];
        first.read_exact(&mut first_buf).unwrap();
        second.read_exact(&mut second_buf).unwrap();
        assert_eq!(&first_buf, b"one");
        assert_eq!(&second_buf, b"two");

        shutdown.shutdown();
        join.join().unwrap().unwrap();
    }

    #[test]
    fn iocp_binds_the_requested_loopback_address() {
        let (addr, shutdown, join) = spawn_echo_on("127.0.0.1:0").unwrap();
        assert!(addr.ip().is_loopback());
        assert_ne!(addr.port(), 0);
        shutdown.shutdown();
        join.join().unwrap().unwrap();
    }

    #[test]
    fn connect_refused_closes_socket_iocp() {
        let dead = {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            drop(listener);
            addr
        };
        let (handle, shutdown, join) = IocpReactor::<EchoHandler>::spawn_client_for_test();
        let err = handle.connect(dead).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
        assert_eq!(handle.stats().connections, 0);
        shutdown.shutdown();
        join.join().unwrap().unwrap();
    }

    #[test]
    fn connect_then_echo_iocp() {
        let (addr, shutdown, join) = spawn_echo();
        let (handle, client_shutdown, client_join) =
            IocpReactor::<ConnectHandler>::spawn_client_for_test();
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

    #[test]
    fn connect_then_echo_ipv6_iocp() {
        let Ok((addr, shutdown, join)) = spawn_echo_on("[::1]:0") else {
            return;
        };
        let (handle, client_shutdown, client_join) =
            IocpReactor::<ConnectHandler>::spawn_client_for_test();
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

    #[test]
    fn stale_completion_after_reuse_is_ignored() {
        let (addr, shutdown, join, control) = spawn_echo_with_control();
        let first = TcpStream::connect(addr).unwrap();
        assert!(control.wait_for_connections(1, std::time::Duration::from_secs(2)));
        let old = control.first_connection();
        // Posts CancelIoEx but leaves the completion to be delivered.
        control.close_no_wait(old);
        // Test-only: free the slot while the completion is still pending.
        control.mark_connection_reusable(old);
        let mut second = TcpStream::connect(addr).unwrap();
        assert!(control.wait_for_connections(1, std::time::Duration::from_secs(2)));
        let new = control.last_connection();
        assert_eq!(new.slot, old.slot, "test did not reuse the old slot");
        // Force the old completion after the new connection exists.
        control.deliver_stale(old);
        second.write_all(b"new").unwrap();
        let mut buf = [0u8; 3];
        second.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"new", "stale completion affected the reused slot");
        drop(first);
        shutdown.shutdown();
        join.join().unwrap().unwrap();
    }

    #[test]
    fn cancel_io_on_close() {
        let (addr, shutdown, join, control) = spawn_echo_with_control();
        let client = TcpStream::connect(addr).unwrap();
        assert!(control.wait_for_connections(1, std::time::Duration::from_secs(2)));
        let id = control.first_connection();
        control.close(id);
        assert!(control.wait_for_connections(0, std::time::Duration::from_secs(2)));
        assert_eq!(control.pending_ops(id), 0);
        drop(client);
        shutdown.shutdown();
        join.join().unwrap().unwrap();
    }
}
