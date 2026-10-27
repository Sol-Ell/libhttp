//! Per-connection reactor state.
//!
//! A [`Conn`] owns the socket, caches its addresses, and holds the bounded
//! outbound queue plus the read-pause flag. It performs no I/O itself: the
//! reactor does the syscalls on the owning thread and only records here the
//! state that decides whether reading and writing may proceed.

use std::collections::VecDeque;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use super::block::{BLOCK_SIZE, Block};
use super::pool::BlockPool;
use super::socket::OwnedSocket;

/// Why [`Conn::enqueue`] did not queue its bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteError {
    /// The outbound queue would exceed its byte cap.
    QueueFull,
    /// The block pool has no allocation left for the copy.
    PoolExhausted,
    /// The connection has already been closed.
    Closed,
}

/// Reactor-side state for one connection.
pub(crate) struct Conn {
    /// `None` only for connections built by the test constructor.
    socket: Option<OwnedSocket>,
    peer_addr: SocketAddr,
    local_addr: SocketAddr,
    /// Maximum number of bytes allowed in `pending_write` at once.
    write_cap: usize,
    /// Provides the blocks the outbound queue is built from.
    pool: Arc<BlockPool>,
    /// Outbound bytes, in the order they were enqueued.
    pending_write: VecDeque<Block>,
    /// Total bytes queued in `pending_write`; also the byte-cap accounting.
    pending_bytes: usize,
    paused: bool,
    closed: bool,
    /// Close as soon as the outbound queue drains (peer EOF with output queued).
    close_after_flush: bool,
}

impl Conn {
    /// Wraps an accepted or connected socket and caches its addresses.
    pub(crate) fn new(
        socket: OwnedSocket,
        pool: Arc<BlockPool>,
        write_cap: usize,
    ) -> io::Result<Self> {
        let peer_addr = socket.peer_addr()?;
        let local_addr = socket.local_addr()?;
        Ok(Self {
            socket: Some(socket),
            peer_addr,
            local_addr,
            write_cap,
            pool,
            pending_write: VecDeque::new(),
            pending_bytes: 0,
            paused: false,
            closed: false,
            close_after_flush: false,
        })
    }

    /// Builds state for tests, with no OS socket and a four-block pool.
    #[cfg(test)]
    pub(crate) fn new_for_test(write_cap: usize) -> Self {
        Self::new_state(None, BlockPool::new(BLOCK_SIZE * 4), write_cap)
    }

    fn new_state(socket: Option<OwnedSocket>, pool: Arc<BlockPool>, write_cap: usize) -> Self {
        let unspecified = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
        Self {
            socket,
            peer_addr: unspecified,
            local_addr: unspecified,
            write_cap,
            pool,
            pending_write: VecDeque::new(),
            pending_bytes: 0,
            paused: false,
            closed: false,
            close_after_flush: false,
        }
    }

    /// The connection's socket.
    ///
    /// # Panics
    ///
    /// Panics for connections built by the test constructor, which have none.
    pub(crate) fn socket(&self) -> &OwnedSocket {
        self.socket
            .as_ref()
            .expect("test-only connection has no socket")
    }

    /// The connection's socket, mutably.
    ///
    /// # Panics
    ///
    /// Panics for connections built by the test constructor, which have none.
    pub(crate) fn socket_mut(&mut self) -> &mut OwnedSocket {
        self.socket
            .as_mut()
            .expect("test-only connection has no socket")
    }

    /// Cached peer address.
    pub(crate) fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Cached local address.
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Copies `data` into pooled blocks on the outbound queue.
    ///
    /// The call is atomic: on error nothing is queued and no block is held. A
    /// failure also pauses reading, because accepting more inbound bytes while
    /// output cannot be queued only grows the backlog. The reactor clears the
    /// pause with [`Self::resume_read`] once the queue has drained.
    pub(crate) fn enqueue(&mut self, data: &[u8]) -> Result<(), WriteError> {
        if self.closed {
            return Err(WriteError::Closed);
        }
        if data.is_empty() {
            return Ok(());
        }

        let total = match self.pending_bytes.checked_add(data.len()) {
            Some(total) => total,
            None => {
                self.paused = true;
                return Err(WriteError::QueueFull);
            }
        };
        if total > self.write_cap {
            self.paused = true;
            return Err(WriteError::QueueFull);
        }

        // Allocate every block before copying anything, so a full pool cannot
        // leave a partially queued write behind.
        let block_count = data.len().div_ceil(BLOCK_SIZE);
        let mut blocks = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            match self.pool.alloc() {
                Some(block) => blocks.push(block),
                None => {
                    self.paused = true;
                    return Err(WriteError::PoolExhausted);
                }
            }
        }

        let mut remaining = data;
        for block in blocks {
            let len = remaining.len().min(BLOCK_SIZE);
            let (mut head, tail) = block.split_at(len);
            head.as_mut_slice().copy_from_slice(&remaining[..len]);
            self.pending_write.push_back(head);
            drop(tail);
            remaining = &remaining[len..];
        }
        debug_assert!(remaining.is_empty());

        self.pending_bytes = total;
        Ok(())
    }

    /// Drains every queued outbound byte, in FIFO order.
    ///
    /// Returning the bytes releases their blocks back to the pool. The pause
    /// flag is left alone: the reactor decides when reading may resume.
    pub(crate) fn take_pending_write(&mut self) -> Option<Vec<u8>> {
        if self.pending_bytes == 0 {
            self.pending_write.clear();
            return None;
        }

        let mut bytes = Vec::with_capacity(self.pending_bytes);
        for block in self.pending_write.drain(..) {
            bytes.extend_from_slice(block.as_slice());
        }
        self.pending_bytes = 0;
        Some(bytes)
    }

    /// Whether any outbound bytes are queued.
    pub(crate) fn has_pending_write(&self) -> bool {
        self.pending_bytes != 0
    }

    /// Stops reading from this connection.
    pub(crate) fn pause_read(&mut self) {
        self.paused = true;
    }

    /// Allows reading again, including after outbound backpressure.
    pub(crate) fn resume_read(&mut self) {
        self.paused = false;
    }

    /// Whether reads are suspended, explicitly or by backpressure.
    pub(crate) fn is_paused(&self) -> bool {
        self.paused
    }

    /// Closes the connection immediately: drops the socket and queued output,
    /// and leaves reads paused.
    ///
    /// Any graceful sequence (flush, `shutdown_write`, read-to-FIN) belongs to
    /// the reactor and must happen before this call.
    pub(crate) fn close(&mut self) {
        self.closed = true;
        self.paused = true;
        self.pending_write.clear();
        self.pending_bytes = 0;
        if let Some(socket) = self.socket.take() {
            socket.close();
        }
    }

    /// Whether [`Self::close`] has been called.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }

    /// Requests close once every queued outbound byte has drained.
    ///
    /// Used on peer EOF when output is still queued: the peer cannot send more,
    /// but it can still receive the echo. The reactor keeps the connection alive
    /// until the queue empties, then calls [`Self::close`]. A plain
    /// [`Self::close`] here would discard those bytes.
    pub(crate) fn close_after_flush(&mut self) {
        self.close_after_flush = true;
    }

    /// Whether a close has been requested for when the outbound queue drains.
    pub(crate) fn is_close_after_flush(&self) -> bool {
        self.close_after_flush
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rt::block::BLOCK_SIZE;
    use crate::rt::pool::BlockPool;
    use crate::rt::socket::OwnedSocket;
    use std::net::{TcpListener, TcpStream};

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

    #[test]
    fn enqueue_is_fifo_and_drains_completely() {
        let mut c = Conn::new_for_test(64);
        c.enqueue(b"abc").unwrap();
        c.enqueue(b"def").unwrap();
        assert!(c.has_pending_write());

        assert_eq!(c.take_pending_write(), Some(b"abcdef".to_vec()));
        assert!(!c.has_pending_write());
        assert!(c.take_pending_write().is_none());
    }

    #[test]
    fn enqueue_spans_multiple_blocks() {
        let mut c = Conn::new_for_test(BLOCK_SIZE * 2);
        let data: Vec<u8> = (0..BLOCK_SIZE + 5).map(|i| i as u8).collect();

        c.enqueue(&data).unwrap();
        assert!(c.has_pending_write());
        assert_eq!(c.take_pending_write(), Some(data));
    }

    #[test]
    fn failed_enqueue_keeps_the_queue_unchanged() {
        let mut c = Conn::new_for_test(8);
        c.enqueue(b"123456").unwrap();

        assert_eq!(c.enqueue(b"789"), Err(WriteError::QueueFull));
        assert_eq!(c.take_pending_write(), Some(b"123456".to_vec()));
    }

    #[test]
    fn pool_exhaustion_is_reported_without_partial_data() {
        let mut c = Conn::new_state(None, BlockPool::new(BLOCK_SIZE), 1024);
        c.enqueue(b"a").unwrap();

        assert_eq!(c.enqueue(b"b"), Err(WriteError::PoolExhausted));
        assert!(c.is_paused());
        assert_eq!(c.take_pending_write(), Some(b"a".to_vec()));
    }

    #[test]
    fn close_rejects_new_writes_and_drops_pending_blocks() {
        let mut c = Conn::new_for_test(16);
        c.enqueue(b"abc").unwrap();
        c.close();

        assert!(c.is_closed());
        assert!(c.is_paused());
        assert_eq!(c.enqueue(b"d"), Err(WriteError::Closed));
        assert!(c.take_pending_write().is_none());
    }

    #[test]
    fn close_after_flush_is_a_deferred_request() {
        let mut c = Conn::new_for_test(16);
        assert!(!c.is_close_after_flush());

        c.close_after_flush();

        assert!(c.is_close_after_flush());
        assert!(!c.is_closed(), "the connection must stay open to flush");
    }

    #[test]
    fn real_connection_caches_socket_addresses() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let local = listener.local_addr().unwrap();
        let client = TcpStream::connect(local).unwrap();
        let client_addr = client.local_addr().unwrap();
        let (server, peer) = listener.accept().unwrap();
        let socket = OwnedSocket::new(server).unwrap();

        let conn = Conn::new(socket, BlockPool::new(BLOCK_SIZE * 4), 64).unwrap();
        assert_eq!(conn.local_addr(), local);
        assert_eq!(conn.peer_addr(), peer);
        assert_eq!(conn.peer_addr(), client_addr);
        assert_eq!(conn.socket().local_addr().unwrap(), local);
    }
}
