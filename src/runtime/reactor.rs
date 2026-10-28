use std::io;
use std::net::SocketAddr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytes::{Buf, BytesMut};
use crossbeam_channel::{Receiver, Sender};
use mio::event::Event;
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token, Waker};

use crate::body::Terminal;
use crate::h1::{BodyFraming, ChunkedDecoder, ChunkedStatus, parse_request_head};
use crate::net;
use crate::server::stream::ServerStream;
use crate::server::{Request, Response, ServerShared};
use crate::{Error, Method, ProtocolErrorKind, StatusCode, TimeoutKind};

use super::RuntimeStatsInner;
use super::exchange::Exchange;
use super::slab::{ConnId, Slab};
use super::timer::{TimerEntry, TimerHeap, TimerKind};
use super::worker::{Job, ReactorWaker, try_dispatch};

const LISTENER_BIT: usize = 1 << (usize::BITS - 1);

/// Commands sent from the runtime or a server handle to a reactor.
pub(crate) enum Command {
    AddListener {
        shared: Arc<ServerShared>,
        listener: TcpListener,
    },
    StopServer {
        server: u64,
        grace: Duration,
    },
    Shutdown {
        grace: Option<Duration>,
    },
}

struct ListenerEntry {
    shared: Arc<ServerShared>,
    listener: TcpListener,
}

/// A reactor control handle.
pub(crate) struct ReactorHandle {
    #[allow(dead_code)]
    pub(crate) id: usize,
    pub(crate) waker: Arc<ReactorWaker>,
    sender: Sender<Command>,
    join: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl ReactorHandle {
    pub(crate) fn send(&self, command: Command) {
        let _ = self.sender.send(command);
        self.waker.wake();
    }

    pub(crate) fn join(&self) {
        let handle = self.join.lock().unwrap().take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }
}

pub(crate) struct Reactor;

impl Reactor {
    pub(crate) fn spawn(
        id: usize,
        stats: Arc<RuntimeStatsInner>,
        worker_tx: Option<Sender<Job>>,
    ) -> io::Result<ReactorHandle> {
        let poll = Poll::new()?;
        let waker = Waker::new(poll.registry(), Token(0))?;
        let reactor_waker = Arc::new(ReactorWaker::new(waker));
        let (sender, receiver) = crossbeam_channel::unbounded();
        let state = ReactorState {
            poll,
            waker: reactor_waker.clone(),
            receiver,
            listeners: Vec::new(),
            conns: Slab::new(),
            timers: TimerHeap::new(),
            worker_tx,
            stats,
            shutting_down: false,
            shutdown_deadline: None,
        };
        let handle = std::thread::Builder::new()
            .name(format!("libhttp-reactor-{id}"))
            .spawn(move || state.run())
            .expect("spawn reactor thread");
        Ok(ReactorHandle {
            id,
            waker: reactor_waker,
            sender,
            join: std::sync::Mutex::new(Some(handle)),
        })
    }
}

struct ReactorState {
    poll: Poll,
    waker: Arc<ReactorWaker>,
    receiver: Receiver<Command>,
    listeners: Vec<Option<ListenerEntry>>,
    conns: Slab<ServerConn>,
    timers: TimerHeap,
    worker_tx: Option<Sender<Job>>,
    stats: Arc<RuntimeStatsInner>,
    shutting_down: bool,
    shutdown_deadline: Option<Instant>,
}

impl ReactorState {
    fn run(mut self) {
        let mut events = Events::with_capacity(1024);
        loop {
            if self.shutting_down
                && (self.conns.len() == 0
                    || self
                        .shutdown_deadline
                        .is_some_and(|deadline| Instant::now() >= deadline))
            {
                break;
            }
            let timeout = self.poll_timeout();
            match self.poll.poll(&mut events, timeout) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
            for event in events.iter() {
                self.handle_event(event);
            }
            self.drain_commands();
            for id in self.waker.drain_ready() {
                self.drive_conn(id);
            }
            self.fire_timers();
        }
        let ids = self.conns.ids();
        for id in ids {
            self.close_conn(id, Terminal::Closed, None);
        }
        self.listeners.clear();
    }

    fn poll_timeout(&self) -> Option<Duration> {
        let mut deadline = self.timers.next_deadline();
        if let Some(shutdown) = self.shutdown_deadline {
            deadline = Some(match deadline {
                Some(current) => current.min(shutdown),
                None => shutdown,
            });
        }
        deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    fn handle_event(&mut self, event: &Event) {
        let token = event.token();
        if token.0 == 0 {
            return;
        }
        if token.0 & LISTENER_BIT != 0 {
            let index = token.0 & !LISTENER_BIT;
            self.accept(index);
            return;
        }
        let Some(id) = ConnId::from_token(token) else {
            return;
        };
        if event.is_error() {
            self.close_conn(id, Terminal::Canceled, None);
            return;
        }
        if let Some(conn) = self.conns.get_mut(id) {
            if event.is_readable() {
                conn.read_ready = true;
            }
            if event.is_writable() {
                conn.write_ready = true;
            }
            if event.is_read_closed() {
                conn.read_eof = true;
            }
            if event.is_write_closed() {
                conn.write_closed = true;
            }
        }
        self.drive_conn(id);
    }

    fn accept(&mut self, index: usize) {
        let Some(entry) = self.listeners.get_mut(index).and_then(Option::take) else {
            return;
        };
        loop {
            match entry.listener.accept() {
                Ok((stream, peer)) => {
                    if entry.shared.shutdown.load(Ordering::Acquire) {
                        continue;
                    }
                    let local = match stream.local_addr() {
                        Ok(local) => local,
                        Err(_) => continue,
                    };
                    if self.conns.len() >= entry.shared.limits.max_connections_per_reactor
                        || entry.shared.active.load(Ordering::Acquire)
                            >= entry.shared.limits.max_connections
                    {
                        continue;
                    }
                    if let Err(error) = net::configure(&stream) {
                        entry.shared.report(&Error::Io(error));
                        continue;
                    }
                    #[cfg(feature = "tls")]
                    let stream = if let Some(tls) = &entry.shared.tls {
                        match ServerStream::tls(stream, tls.rustls_config()) {
                            Ok(stream) => stream,
                            Err(error) => {
                                entry.shared.report(&error);
                                continue;
                            }
                        }
                    } else {
                        ServerStream::plain(stream)
                    };
                    #[cfg(not(feature = "tls"))]
                    let stream = ServerStream::plain(stream);
                    entry.shared.connection_open();
                    self.stats
                        .active_connections
                        .fetch_add(1, Ordering::Relaxed);
                    let conn = ServerConn::new(
                        stream,
                        entry.shared.clone(),
                        peer,
                        Some(local),
                        self.waker.clone(),
                    );
                    let id = self.conns.insert(conn);
                    {
                        let conn = self.conns.get_mut(id).unwrap();
                        if let Err(error) = self.poll.registry().register(
                            conn.stream.tcp_mut(),
                            id.token(),
                            Interest::READABLE,
                        ) {
                            entry.shared.report(&Error::Io(error));
                            self.close_conn(id, Terminal::Canceled, None);
                            continue;
                        }
                        conn.id = id;
                        conn.registered = true;
                        conn.read_interest = true;
                    }
                    self.timers.insert(TimerEntry {
                        deadline: Instant::now() + entry.shared.timeouts.header_read,
                        conn: id,
                        seq: 0,
                        kind: TimerKind::HeaderRead,
                    });
                    if self
                        .conns
                        .get(id)
                        .is_some_and(|conn| conn.stream.is_handshaking())
                    {
                        self.timers.insert(TimerEntry {
                            deadline: Instant::now() + entry.shared.timeouts.tls_handshake,
                            conn: id,
                            seq: 0,
                            kind: TimerKind::TlsHandshake,
                        });
                    }
                }
                Err(error) if net::is_would_block(&error) => break,
                Err(error) => {
                    entry.shared.report(&Error::Io(error));
                    break;
                }
            }
        }
        if let Some(slot) = self.listeners.get_mut(index) {
            *slot = Some(entry);
        }
    }

    fn drive_conn(&mut self, id: ConnId) {
        let Some(conn) = self.conns.get_mut(id) else {
            return;
        };
        let outcome = conn.drive(self.worker_tx.as_ref());
        for timer in outcome.timers {
            self.timers.insert(TimerEntry {
                deadline: timer.deadline,
                conn: id,
                seq: timer.seq,
                kind: timer.kind,
            });
        }
        let close = outcome.close;
        let read = outcome.read;
        let write = outcome.write;
        if close {
            self.close_conn(id, Terminal::Canceled, None);
            return;
        }
        let Some(conn) = self.conns.get_mut(id) else {
            return;
        };
        if read || write {
            let interest = match (read, write) {
                (true, true) => Interest::READABLE.add(Interest::WRITABLE),
                (true, false) => Interest::READABLE,
                (false, true) => Interest::WRITABLE,
                (false, false) => unreachable!(),
            };
            let changed = conn.read_interest != read || conn.write_interest != write;
            if !conn.registered {
                if let Err(error) =
                    self.poll
                        .registry()
                        .register(conn.stream.tcp_mut(), id.token(), interest)
                {
                    conn.shared.report(&Error::Io(error));
                    self.close_conn(id, Terminal::Canceled, None);
                    return;
                }
                conn.registered = true;
                conn.read_interest = read;
                conn.write_interest = write;
            } else if changed {
                if let Err(error) =
                    self.poll
                        .registry()
                        .reregister(conn.stream.tcp_mut(), id.token(), interest)
                {
                    conn.shared.report(&Error::Io(error));
                    self.close_conn(id, Terminal::Canceled, None);
                    return;
                }
                conn.read_interest = read;
                conn.write_interest = write;
            }
        } else if conn.registered {
            let _ = self.poll.registry().deregister(conn.stream.tcp_mut());
            conn.registered = false;
            conn.read_interest = false;
            conn.write_interest = false;
        }
    }

    fn close_conn(&mut self, id: ConnId, terminal: Terminal, report: Option<Error>) {
        let Some(mut conn) = self.conns.remove(id) else {
            return;
        };
        if conn.registered {
            let _ = self.poll.registry().deregister(conn.stream.tcp_mut());
        }
        conn.stream.shutdown();
        if let Some(exchange) = conn.exchange.take() {
            exchange.terminal(terminal);
        }
        conn.shared.connection_closed();
        self.stats
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
        conn.shared.stats.resets.fetch_add(1, Ordering::Relaxed);
        if let Some(error) = report {
            conn.shared.report(&error);
        }
    }

    fn drain_commands(&mut self) {
        while let Ok(command) = self.receiver.try_recv() {
            match command {
                Command::AddListener { shared, listener } => {
                    let index = self.listeners.len();
                    self.listeners
                        .push(Some(ListenerEntry { shared, listener }));
                    let token = Token(LISTENER_BIT | index);
                    let entry = self.listeners[index].as_mut().unwrap();
                    if let Err(error) = self.poll.registry().register(
                        &mut entry.listener,
                        token,
                        Interest::READABLE,
                    ) {
                        entry.shared.report(&Error::Io(error));
                    }
                }
                Command::StopServer { server, grace } => self.stop_server(server, grace),
                Command::Shutdown { grace } => self.begin_shutdown(grace),
            }
        }
    }

    fn stop_server(&mut self, server: u64, grace: Duration) {
        for slot in &mut self.listeners {
            if slot.as_ref().is_some_and(|entry| entry.shared.id == server) {
                if let Some(mut entry) = slot.take() {
                    let _ = self.poll.registry().deregister(&mut entry.listener);
                }
            }
        }
        let ids = self.conns.ids();
        let deadline = Instant::now() + grace;
        for id in ids {
            let Some(conn) = self.conns.get_mut(id) else {
                continue;
            };
            if conn.shared.id != server {
                continue;
            }
            conn.shutdown = true;
            if conn.phase == Phase::ReadingHead || grace.is_zero() {
                self.close_conn(id, Terminal::Closed, None);
            } else {
                self.timers.insert(TimerEntry {
                    deadline,
                    conn: id,
                    seq: conn.seq,
                    kind: TimerKind::ServerGrace,
                });
            }
        }
    }

    fn begin_shutdown(&mut self, grace: Option<Duration>) {
        self.shutting_down = true;
        let grace = grace.unwrap_or(Duration::ZERO);
        self.shutdown_deadline = Some(Instant::now() + grace);
        for slot in &mut self.listeners {
            if let Some(mut entry) = slot.take() {
                let _ = self.poll.registry().deregister(&mut entry.listener);
            }
        }
        let ids = self.conns.ids();
        for id in ids {
            let Some(conn) = self.conns.get_mut(id) else {
                continue;
            };
            conn.shutdown = true;
            if conn.phase == Phase::ReadingHead || grace.is_zero() {
                self.close_conn(id, Terminal::Closed, None);
            } else {
                self.timers.insert(TimerEntry {
                    deadline: Instant::now() + grace,
                    conn: id,
                    seq: conn.seq,
                    kind: TimerKind::ServerGrace,
                });
            }
        }
    }

    fn fire_timers(&mut self) {
        let now = Instant::now();
        let due = self.timers.pop_due(now);
        for entry in due {
            let Some(conn) = self.conns.get(entry.conn) else {
                continue;
            };
            match entry.kind {
                TimerKind::HeaderRead | TimerKind::KeepAlive => {
                    if conn.phase == Phase::ReadingHead && conn.seq == entry.seq {
                        self.close_conn(
                            entry.conn,
                            Terminal::Timeout(TimeoutKind::HeaderRead),
                            None,
                        );
                    }
                }
                TimerKind::BodyIdle => {
                    let stale = conn.body_deadline.is_some_and(|deadline| deadline > now);
                    if stale {
                        continue;
                    }
                    if let Some(exchange) = &conn.exchange {
                        if exchange.seq == entry.seq && !conn.body_done {
                            exchange.timeout(TimeoutKind::BodyIdle);
                            self.close_conn(
                                entry.conn,
                                Terminal::Timeout(TimeoutKind::BodyIdle),
                                None,
                            );
                        }
                    }
                }
                TimerKind::Total => {
                    if let Some(exchange) = &conn.exchange {
                        if exchange.seq == entry.seq {
                            exchange.timeout(TimeoutKind::Total);
                            self.close_conn(
                                entry.conn,
                                Terminal::Timeout(TimeoutKind::Total),
                                None,
                            );
                        }
                    }
                }
                TimerKind::Deferred => {
                    if let Some(exchange) = &conn.exchange {
                        if exchange.seq == entry.seq && !exchange.is_finished() {
                            exchange.fail_overloaded(StatusCode::GATEWAY_TIMEOUT);
                            self.drive_conn(entry.conn);
                        }
                    }
                }
                TimerKind::ServerGrace => {
                    self.close_conn(entry.conn, Terminal::Closed, None);
                }
                TimerKind::TlsHandshake => {
                    if conn.stream.is_handshaking() {
                        self.close_conn(
                            entry.conn,
                            Terminal::Timeout(TimeoutKind::TlsHandshake),
                            None,
                        );
                    }
                }
                TimerKind::ReadStall | TimerKind::WriteStall | TimerKind::Connect => {}
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    ReadingHead,
    Active,
    Closing,
}

struct TimerRequest {
    deadline: Instant,
    kind: TimerKind,
    seq: u64,
}

struct DriveOutcome {
    close: bool,
    read: bool,
    write: bool,
    timers: Vec<TimerRequest>,
}

struct ServerConn {
    id: ConnId,
    stream: ServerStream,
    shared: Arc<ServerShared>,
    peer: Option<SocketAddr>,
    local: Option<SocketAddr>,
    waker: Arc<ReactorWaker>,
    read_buf: BytesMut,
    write_buf: BytesMut,
    phase: Phase,
    exchange: Option<Arc<Exchange>>,
    seq: u64,
    read_eof: bool,
    body_done: bool,
    body_remaining: u64,
    chunked: Option<ChunkedDecoder>,
    registered: bool,
    read_interest: bool,
    write_interest: bool,
    read_ready: bool,
    write_ready: bool,
    write_closed: bool,
    close_after_write: bool,
    shutdown: bool,
    body_deadline: Option<Instant>,
}

impl ServerConn {
    fn new(
        stream: ServerStream,
        shared: Arc<ServerShared>,
        peer: SocketAddr,
        local: Option<SocketAddr>,
        waker: Arc<ReactorWaker>,
    ) -> Self {
        Self {
            id: ConnId {
                slot: u32::MAX,
                generation: 0,
            },
            stream,
            shared,
            peer: Some(peer),
            local,
            waker,
            read_buf: BytesMut::with_capacity(16 * 1024),
            write_buf: BytesMut::with_capacity(16 * 1024),
            phase: Phase::ReadingHead,
            exchange: None,
            seq: 0,
            read_eof: false,
            body_done: false,
            body_remaining: 0,
            chunked: None,
            registered: false,
            read_interest: false,
            write_interest: false,
            read_ready: false,
            write_ready: false,
            write_closed: false,
            close_after_write: false,
            shutdown: false,
            body_deadline: None,
        }
    }

    fn drive(&mut self, worker_tx: Option<&Sender<Job>>) -> DriveOutcome {
        let mut timers = Vec::new();
        let mut close = false;
        self.read_ready = false;
        self.write_ready = false;

        if self.shutdown && self.phase == Phase::ReadingHead {
            return DriveOutcome {
                close: true,
                read: false,
                write: false,
                timers,
            };
        }

        self.drain_out();
        if self.write_socket().is_err() {
            close = true;
        }

        if !close && !self.read_eof && self.read_socket().is_err() {
            close = true;
        }

        if !close {
            self.process(worker_tx, &mut timers, &mut close);
        }

        self.drain_out();
        if self.write_socket().is_err() {
            close = true;
        }

        if self.write_closed {
            close = true;
        }
        if self.close_after_write && self.write_buf.is_empty() && !self.stream.wants_write() {
            close = true;
        }

        let read = !close
            && !self.read_eof
            && (self.stream.is_handshaking()
                || match self.phase {
                    Phase::ReadingHead => true,
                    Phase::Active => {
                        !self.body_done
                            && self.exchange.as_ref().is_some_and(|exchange| {
                                exchange.in_body.available() < exchange.in_body.limit()
                            })
                    }
                    Phase::Closing => false,
                });
        let write = !close
            && (!self.write_buf.is_empty()
                || self.stream.wants_write()
                || self
                    .exchange
                    .as_ref()
                    .is_some_and(|exchange| exchange.out_body.available() > 0));
        DriveOutcome {
            close,
            read,
            write,
            timers,
        }
    }

    fn process(
        &mut self,
        worker_tx: Option<&Sender<Job>>,
        timers: &mut Vec<TimerRequest>,
        close: &mut bool,
    ) {
        loop {
            match self.phase {
                Phase::ReadingHead => {
                    if self.read_eof {
                        *close = true;
                        return;
                    }
                    if !self.parse_head(worker_tx, timers, close) {
                        return;
                    }
                }
                Phase::Active => {
                    self.process_active(timers, close);
                    if self.phase == Phase::ReadingHead && !self.read_buf.is_empty() {
                        continue;
                    }
                    return;
                }
                Phase::Closing => return,
            }
        }
    }

    fn parse_head(
        &mut self,
        worker_tx: Option<&Sender<Job>>,
        timers: &mut Vec<TimerRequest>,
        close: &mut bool,
    ) -> bool {
        let (head, consumed) = match parse_request_head(&self.read_buf, &self.shared.limits) {
            Ok(Some(parsed)) => parsed,
            Ok(None) => return false,
            Err(error) => {
                self.send_error_status(error_status(&error));
                *close = true;
                return false;
            }
        };
        let _ = self.read_buf.split_to(consumed);
        self.seq += 1;
        self.shared.stats.requests.fetch_add(1, Ordering::Relaxed);
        let exchange = Exchange::new(
            self.seq,
            self.id,
            self.shared.clone(),
            head.method.clone(),
            head.version,
            head.keep_alive,
            head.framing,
            head.expect_continue,
            self.waker.clone(),
            self.peer,
            self.local,
        );

        if head.expect_unsupported {
            exchange.fail_overloaded(StatusCode::EXPECTATION_FAILED);
            self.exchange = Some(exchange);
            self.phase = Phase::Closing;
            self.close_after_write = true;
            return false;
        }
        if head.method == Method::Connect {
            exchange.fail_overloaded(StatusCode::NOT_IMPLEMENTED);
            self.exchange = Some(exchange);
            self.phase = Phase::Closing;
            self.close_after_write = true;
            return false;
        }

        let mut head = head;
        if head.decode_headers(&self.shared.registry).is_err() {
            exchange.fail_overloaded(StatusCode::BAD_REQUEST);
            self.exchange = Some(exchange);
            self.phase = Phase::Closing;
            self.close_after_write = true;
            return false;
        }

        let request = Request::new(exchange.clone(), head);
        let response = Response::attached(exchange.clone());
        let handler = self.shared.handler();
        let job: Job = Box::new(move || {
            let mut request = request;
            let mut response = response;
            let result = catch_unwind(AssertUnwindSafe(|| {
                handler.handle(&mut request, &mut response)
            }));
            let result = match result {
                Ok(result) => result,
                Err(_) => Err(Error::InvalidRequest("handler panicked")),
            };
            response.finalize(result);
        });
        let dispatched = worker_tx
            .and_then(|sender| try_dispatch(sender, job).ok())
            .is_some();
        if !dispatched {
            exchange.fail_overloaded(StatusCode::SERVICE_UNAVAILABLE);
            self.exchange = Some(exchange);
            self.phase = Phase::Closing;
            self.close_after_write = true;
            return false;
        }
        if exchange.expect_continue {
            exchange.mark_continue_sent();
        }
        self.phase = Phase::Active;
        self.exchange = Some(exchange.clone());
        self.body_done = false;
        self.body_remaining = match exchange.framing {
            BodyFraming::None => 0,
            BodyFraming::ContentLength(length) => length,
            BodyFraming::Chunked => 0,
            BodyFraming::UntilClose => 0,
        };
        self.chunked = matches!(exchange.framing, BodyFraming::Chunked).then(ChunkedDecoder::new);
        if exchange.framing == BodyFraming::None {
            self.body_done = true;
            exchange.mark_request_body_eof();
        }
        if let Some(total) = self.shared.timeouts.total_request {
            timers.push(TimerRequest {
                deadline: Instant::now() + total,
                kind: TimerKind::Total,
                seq: self.seq,
            });
        }
        if !self.body_done {
            let deadline = Instant::now() + self.shared.timeouts.body_idle;
            self.body_deadline = Some(deadline);
            timers.push(TimerRequest {
                deadline,
                kind: TimerKind::BodyIdle,
                seq: self.seq,
            });
        }
        true
    }

    fn process_active(&mut self, timers: &mut Vec<TimerRequest>, close: &mut bool) {
        self.feed_body(timers, close);
        self.drain_out();
        if self.write_socket().is_err() {
            *close = true;
        }
        let Some(exchange) = self.exchange.clone() else {
            return;
        };
        let defer_deadline = {
            let mut meta = exchange.meta.lock().unwrap();
            if meta.deferred && !meta.deferred_timer_armed {
                meta.deferred_timer_armed = true;
                self.shared
                    .timeouts
                    .deferred
                    .map(|timeout| Instant::now() + timeout)
            } else {
                None
            }
        };
        if let Some(deadline) = defer_deadline {
            timers.push(TimerRequest {
                deadline,
                kind: TimerKind::Deferred,
                seq: self.seq,
            });
        }
        if exchange.is_terminal() {
            self.phase = Phase::Closing;
            self.close_after_write = true;
            return;
        }
        if exchange.response_done() {
            let close_connection =
                self.shutdown || exchange.close_after_response() || self.read_eof;
            if close_connection {
                self.phase = Phase::Closing;
                self.close_after_write = true;
            } else {
                self.exchange = None;
                self.phase = Phase::ReadingHead;
                self.body_done = false;
                self.body_remaining = 0;
                self.chunked = None;
                self.body_deadline = None;
                timers.push(TimerRequest {
                    deadline: Instant::now() + self.shared.timeouts.keep_alive,
                    kind: TimerKind::KeepAlive,
                    seq: self.seq,
                });
            }
        }
    }

    fn feed_body(&mut self, timers: &mut Vec<TimerRequest>, close: &mut bool) {
        if self.body_done {
            return;
        }
        let Some(exchange) = self.exchange.clone() else {
            return;
        };
        let limit = exchange.in_body.limit();
        match exchange.framing {
            BodyFraming::None => {
                self.body_done = true;
                exchange.mark_request_body_eof();
            }
            BodyFraming::ContentLength(_) => {
                while self.body_remaining > 0 && !self.read_buf.is_empty() {
                    if exchange.in_body.available() >= limit {
                        break;
                    }
                    let available = limit - exchange.in_body.available();
                    let take = self
                        .body_remaining
                        .min(self.read_buf.len() as u64)
                        .min(available as u64) as usize;
                    if take == 0 {
                        break;
                    }
                    let chunk = self.read_buf.split_to(take).freeze();
                    if !exchange.in_body.push(chunk) {
                        break;
                    }
                    self.body_remaining -= take as u64;
                    self.touch_body(timers);
                }
                if self.body_remaining == 0 {
                    self.body_done = true;
                    exchange.mark_request_body_eof();
                }
            }
            BodyFraming::Chunked => {
                let Some(mut decoder) = self.chunked.take() else {
                    *close = true;
                    return;
                };
                let status = if exchange.in_body.available() < limit {
                    decoder.feed(
                        &mut self.read_buf,
                        &exchange.in_body,
                        self.shared.limits.max_trailer_bytes,
                    )
                } else {
                    Ok(ChunkedStatus::NeedMore)
                };
                match status {
                    Ok(ChunkedStatus::Done) => {
                        exchange.set_trailers(decoder.trailers().clone());
                        self.body_done = true;
                        exchange.mark_request_body_eof();
                        self.touch_body(timers);
                    }
                    Ok(ChunkedStatus::NeedMore) => {
                        self.chunked = Some(decoder);
                        self.touch_body(timers);
                    }
                    Err(_) => {
                        exchange.terminal(Terminal::InvalidRequest("invalid chunked body"));
                        self.body_done = true;
                        *close = true;
                        return;
                    }
                }
            }
            BodyFraming::UntilClose => {
                while !self.read_buf.is_empty() && exchange.in_body.available() < limit {
                    let available = limit - exchange.in_body.available();
                    let take = self.read_buf.len().min(available);
                    let chunk = self.read_buf.split_to(take).freeze();
                    if !exchange.in_body.push(chunk) {
                        break;
                    }
                    self.touch_body(timers);
                }
            }
        }
        if let Some(limit) = self.shared.limits.max_body_bytes {
            if exchange.in_body.written_bytes() > limit {
                exchange.fail_overloaded(StatusCode::PAYLOAD_TOO_LARGE);
                self.phase = Phase::Closing;
                self.close_after_write = true;
                self.body_done = true;
                *close = true;
                return;
            }
        }
        if self.read_eof && !self.body_done {
            exchange.terminal(Terminal::Canceled);
            self.body_done = true;
            *close = true;
        }
    }

    fn touch_body(&mut self, timers: &mut Vec<TimerRequest>) {
        if self.body_done {
            return;
        }
        let deadline = Instant::now() + self.shared.timeouts.body_idle;
        self.body_deadline = Some(deadline);
        timers.push(TimerRequest {
            deadline,
            kind: TimerKind::BodyIdle,
            seq: self.seq,
        });
    }

    fn read_socket(&mut self) -> Result<(), Error> {
        for _ in 0..4 {
            match self.stream.read_plain(&mut self.read_buf) {
                Ok(result) => {
                    self.shared
                        .stats
                        .bytes_read
                        .fetch_add(result.bytes as u64, Ordering::Relaxed);
                    if result.eof {
                        self.read_eof = true;
                        return Ok(());
                    }
                    if result.bytes == 0 || self.read_buf.len() > 64 * 1024 {
                        break;
                    }
                }
                Err(Error::Io(error)) if net::is_would_block(&error) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn write_socket(&mut self) -> Result<(), Error> {
        while !self.write_buf.is_empty() {
            match self.stream.write_plain(&self.write_buf) {
                Ok(0) => return Ok(()),
                Ok(written) => {
                    self.write_buf.advance(written);
                    self.shared
                        .stats
                        .bytes_written
                        .fetch_add(written as u64, Ordering::Relaxed);
                }
                Err(Error::Io(error)) if net::is_would_block(&error) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
        self.stream.flush()
    }

    fn drain_out(&mut self) {
        let Some(exchange) = &self.exchange else {
            return;
        };
        while let Some(chunk) = exchange.out_body.take() {
            self.write_buf.extend_from_slice(&chunk);
            if self.write_buf.len() >= 256 * 1024 {
                break;
            }
        }
    }

    fn send_error_status(&mut self, status: StatusCode) {
        let text = format!(
            "HTTP/1.1 {} {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            status.as_u16(),
            status.canonical_reason()
        );
        self.write_buf.extend_from_slice(text.as_bytes());
        self.phase = Phase::Closing;
        self.close_after_write = true;
    }
}

fn error_status(error: &Error) -> StatusCode {
    match error {
        Error::HeadTooLarge { .. } => StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
        Error::BodyTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        Error::Protocol(protocol) => match protocol.kind() {
            ProtocolErrorKind::Unsupported => StatusCode::HTTP_VERSION_NOT_SUPPORTED,
            _ => StatusCode::BAD_REQUEST,
        },
        _ => StatusCode::BAD_REQUEST,
    }
}
