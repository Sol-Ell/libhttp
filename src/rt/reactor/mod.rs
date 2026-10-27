//! Shared reactor state and connection lifecycle rules.
//!
//! The platform reactors in [`unix`] and [`iocp`] own the OS wait loop and the
//! socket syscalls. This module owns the state that must behave identically on
//! every platform: the connection slab, the block pool, the timer heap and the
//! [`Handler`]. Platform loops call the `on_*` helpers after their I/O, and this
//! module applies the close, error and pause rules in one place.
//!
//! A connection is addressed by its generation-checked [`ConnId`]. Every helper
//! accepts an id that may already be stale (an event that raced with a close)
//! and ignores it, so platform loops never need to reason about that race.
//!
//! A handler error is terminal for the connection: the helper closes it, calls
//! [`Handler::on_close`] and returns the error to the caller. The platform loop
//! should treat that error as per-connection and keep running.

use std::io;
use std::sync::Arc;

use crate::rt::block::BLOCK_SIZE;
use crate::rt::conn::Conn;
use crate::rt::handler::Handler;
use crate::rt::pool::BlockPool;
use crate::rt::slab::{ConnId, Slab};
use crate::rt::timer::TimerHeap;

pub(crate) mod iocp;
pub(crate) mod unix;

/// Default block-pool capacity for one reactor: 5,120 blocks (80 MiB).
///
/// This is the runtime design's per-reactor default. The pool grows lazily, so
/// idle connections do not allocate any of it.
const DEFAULT_POOL_BYTES: usize = BLOCK_SIZE * 5_120;

/// State shared by the Unix and Windows reactors.
pub(crate) struct ReactorCore<H: Handler> {
    conns: Slab<Conn>,
    pool: Arc<BlockPool>,
    timers: TimerHeap,
    handler: H,
    /// Test-only command/observation channel installed by the harness.
    #[cfg(test)]
    control: Option<Arc<crate::rt::test_support::ControlChannel>>,
}

impl<H: Handler> ReactorCore<H> {
    /// Builds a core with an empty slab, an empty timer heap and a fresh pool.
    pub(crate) fn new(handler: H) -> Self {
        Self {
            conns: Slab::new(),
            pool: BlockPool::new(DEFAULT_POOL_BYTES),
            timers: TimerHeap::new(),
            handler,
            #[cfg(test)]
            control: None,
        }
    }

    /// Installs a test control channel. The harness shares it with the reactor.
    #[cfg(test)]
    pub(crate) fn attach_control(&mut self, control: Arc<crate::rt::test_support::ControlChannel>) {
        self.control = Some(control);
    }

    /// Records delivered plaintext bytes in the test control channel, if any.
    #[cfg(test)]
    pub(crate) fn note_read(&mut self, bytes: usize) {
        if let Some(control) = &self.control {
            control.note_read(bytes);
        }
    }

    /// Number of live connections.
    pub(crate) fn len(&self) -> usize {
        self.conns.len()
    }

    /// Whether the slab holds no connections.
    pub(crate) fn is_empty(&self) -> bool {
        self.conns.is_empty()
    }

    /// The pool every connection in this core allocates from.
    pub(crate) fn pool(&self) -> &Arc<BlockPool> {
        &self.pool
    }

    /// The reactor's timer heap.
    pub(crate) fn timers(&self) -> &TimerHeap {
        &self.timers
    }

    /// The reactor's timer heap, mutably.
    pub(crate) fn timers_mut(&mut self) -> &mut TimerHeap {
        &mut self.timers
    }

    /// The installed handler.
    ///
    /// Test-only observation point for handler state after callbacks run.
    #[cfg(test)]
    pub(crate) fn handler(&self) -> &H {
        &self.handler
    }

    /// Inserts a connection and returns its generation-checked id.
    ///
    /// The connection is not reported to the handler yet; call
    /// [`Self::on_accept`] or [`Self::on_connect`] after the platform reactor
    /// has registered the socket.
    pub(crate) fn insert(&mut self, conn: Conn) -> ConnId {
        #[cfg(test)]
        let addresses = (conn.local_addr(), conn.peer_addr());
        let id = self.conns.insert(conn);
        #[cfg(test)]
        {
            crate::rt::test_support::note_connection_opened();
            if let Some(control) = &self.control {
                control.note_open(id, addresses.0, addresses.1);
            }
        }
        id
    }

    /// Looks up a live connection.
    pub(crate) fn conn(&self, id: ConnId) -> Option<&Conn> {
        self.conns.get(id)
    }

    /// Looks up a live connection, mutably.
    ///
    /// Platform reactors use this to perform the socket reads and writes that
    /// the core itself does not do.
    pub(crate) fn conn_mut(&mut self, id: ConnId) -> Option<&mut Conn> {
        self.conns.get_mut(id)
    }

    /// Reports a newly accepted connection to the handler.
    pub(crate) fn on_accept(&mut self, id: ConnId) -> io::Result<()> {
        let result = match self.conns.get_mut(id) {
            Some(conn) => self.handler.on_accept(conn),
            None => return Ok(()),
        };
        self.finish(id, result)
    }

    /// Reports an established outbound connection to the handler.
    pub(crate) fn on_connect(&mut self, id: ConnId) -> io::Result<()> {
        let result = match self.conns.get_mut(id) {
            Some(conn) => self.handler.on_connect(conn),
            None => return Ok(()),
        };
        self.finish(id, result)
    }

    /// Gives `data` to the handler.
    ///
    /// The caller has already read `data` from the connection's socket and
    /// checked that reading is allowed; this helper performs no I/O.
    pub(crate) fn on_read_ready(&mut self, id: ConnId, data: &[u8]) -> io::Result<()> {
        let result = match self.conns.get_mut(id) {
            Some(conn) => self.handler.on_read(conn, data),
            None => return Ok(()),
        };
        self.finish(id, result)
    }

    /// Applies the post-write state rules after the platform flushed output.
    ///
    /// The platform reactor performs the actual socket writes and calls this
    /// helper once the queue has drained. If EOF was seen while output was
    /// still queued, the deferred close now runs. Otherwise the handler gets a
    /// chance to retry output it carried during backpressure; when nothing is
    /// queued or carried, backpressure is cleared so the reactor can re-arm
    /// reads. An explicit pause is never cleared here. This helper performs no
    /// I/O.
    pub(crate) fn on_write_ready(&mut self, id: ConnId) -> io::Result<()> {
        let Some(conn) = self.conns.get(id) else {
            return Ok(());
        };
        if conn.has_pending_write() {
            return Ok(());
        }

        // The handler may have carried a chunk it could not enqueue. Retry it
        // now that there is room; a deferred EOF close must not drop it.
        let retried = match self.conns.get_mut(id) {
            Some(conn) => Some(self.handler.on_output_ready(conn)),
            None => None,
        };
        if let Some(Err(err)) = retried {
            // A failed retry is terminal for this connection only.
            let _ = self.finish(id, Err(err));
            return Ok(());
        }

        let (pending, carry, close_after_flush) = match self.conns.get(id) {
            Some(conn) => (
                conn.has_pending_write(),
                conn.has_carry(),
                conn.is_close_after_flush(),
            ),
            None => return Ok(()),
        };
        if close_after_flush {
            if !pending && !carry {
                self.close(id, "peer-eof");
            }
            return Ok(());
        }
        if !pending && !carry {
            if let Some(conn) = self.conns.get_mut(id) {
                conn.clear_backpressure();
            }
        }
        Ok(())
    }

    /// Handles peer EOF.
    ///
    /// If output is still queued the close is deferred until the queue drains:
    /// the peer has half-closed its sending side but can still receive the echo.
    /// Closing immediately would discard the queued bytes.
    pub(crate) fn on_eof(&mut self, id: ConnId) {
        if let Some(conn) = self.conns.get_mut(id) {
            // A carried chunk is outbound work even though it is not in the
            // queue yet; closing now would truncate the echo.
            if conn.has_pending_write() || conn.has_carry() {
                conn.close_after_flush();
                return;
            }
        }
        self.close(id, "peer-eof");
    }

    /// Closes `id`, releasing its socket and queued output, then reports it.
    ///
    /// Removing the slab entry first bumps the generation, so late events for
    /// this connection are ignored even while `on_close` runs. A stale id is a
    /// no-op.
    pub(crate) fn close(&mut self, id: ConnId, reason: &'static str) {
        debug_assert!(!reason.is_empty(), "close reason must be non-empty");
        let Some(mut conn) = self.conns.remove(id) else {
            return;
        };
        conn.close();
        #[cfg(test)]
        {
            crate::rt::test_support::note_connection_closed();
            if let Some(control) = &self.control {
                control.note_closed(id);
            }
        }
        self.handler.on_close(id);
    }

    /// Closes the connection if the handler callback failed.
    fn finish(&mut self, id: ConnId, result: io::Result<()>) -> io::Result<()> {
        match result {
            Ok(()) => Ok(()),
            Err(err) => {
                self.close(id, "handler-error");
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rt::conn::Conn;
    use crate::rt::handler::{EchoHandler, Handler};
    use crate::rt::slab::ConnId;
    use crate::rt::timer::TimerKey;
    use std::io;
    use std::time::Instant;

    /// Handler that records closes and can fail a chosen callback.
    struct FailingHandler {
        closed: Vec<ConnId>,
        fail_on_accept: bool,
        fail_on_connect: bool,
        fail_on_read: bool,
    }

    impl Handler for FailingHandler {
        fn on_accept(&mut self, _conn: &mut Conn) -> io::Result<()> {
            if self.fail_on_accept {
                return Err(io::Error::other("accept failed"));
            }
            Ok(())
        }

        fn on_connect(&mut self, _conn: &mut Conn) -> io::Result<()> {
            if self.fail_on_connect {
                return Err(io::Error::other("connect failed"));
            }
            Ok(())
        }

        fn on_read(&mut self, _conn: &mut Conn, _data: &[u8]) -> io::Result<()> {
            if self.fail_on_read {
                return Err(io::Error::other("read failed"));
            }
            Ok(())
        }

        fn on_close(&mut self, id: ConnId) {
            self.closed.push(id);
        }
    }

    impl FailingHandler {
        fn new() -> Self {
            Self {
                closed: Vec::new(),
                fail_on_accept: false,
                fail_on_connect: false,
                fail_on_read: false,
            }
        }
    }

    #[test]
    fn core_tracks_connection_lifecycle() {
        let mut core = ReactorCore::new(EchoHandler);
        assert!(core.is_empty());

        let id = core.insert(Conn::new_for_test(64));
        assert_eq!(core.len(), 1);
        assert!(core.conn(id).is_some());
        assert!(core.conn_mut(id).is_some());

        core.on_accept(id).unwrap();
        core.on_connect(id).unwrap();
        core.close(id, "test");

        assert!(core.is_empty());
        assert!(core.conn(id).is_none());
        assert!(core.conn_mut(id).is_none());
    }

    #[test]
    fn read_ready_runs_the_handler_and_queues_echo_output() {
        let mut core = ReactorCore::new(EchoHandler);
        let id = core.insert(Conn::new_for_test(64));

        core.on_read_ready(id, b"hello").unwrap();

        assert_eq!(
            core.conn_mut(id).unwrap().take_pending_write(),
            Some(b"hello".to_vec())
        );
    }

    #[test]
    fn handler_read_error_closes_the_connection_and_reports_on_close() {
        let mut core = ReactorCore::new(FailingHandler {
            fail_on_read: true,
            ..FailingHandler::new()
        });
        let id = core.insert(Conn::new_for_test(64));

        let err = core.on_read_ready(id, b"x").unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(core.is_empty());
        assert_eq!(core.handler().closed, vec![id]);
    }

    #[test]
    fn handler_accept_error_closes_the_connection() {
        let mut core = ReactorCore::new(FailingHandler {
            fail_on_accept: true,
            ..FailingHandler::new()
        });
        let id = core.insert(Conn::new_for_test(64));

        let err = core.on_accept(id).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(core.is_empty());
        assert_eq!(core.handler().closed, vec![id]);
    }

    #[test]
    fn handler_connect_error_closes_the_connection() {
        let mut core = ReactorCore::new(FailingHandler {
            fail_on_connect: true,
            ..FailingHandler::new()
        });
        let id = core.insert(Conn::new_for_test(64));

        let err = core.on_connect(id).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(core.is_empty());
        assert_eq!(core.handler().closed, vec![id]);
    }

    #[test]
    fn echo_overflow_pauses_and_carries_instead_of_resetting() {
        let mut core = ReactorCore::new(EchoHandler);
        let id = core.insert(Conn::new_for_test(8));

        core.on_read_ready(id, b"12345").unwrap();
        core.on_read_ready(id, b"67890").unwrap();

        let conn = core.conn(id).unwrap();
        assert!(conn.is_backpressured());
        assert!(!conn.is_read_paused());
        assert!(conn.has_carry());
        assert_eq!(core.len(), 1, "overflow must pause, not reset");
        assert_eq!(
            core.conn_mut(id).unwrap().take_pending_write(),
            Some(b"12345".to_vec())
        );

        // Draining the queue retries the carried chunk through the handler.
        core.on_write_ready(id).unwrap();
        assert_eq!(
            core.conn_mut(id).unwrap().take_pending_write(),
            Some(b"67890".to_vec())
        );
        assert_eq!(core.len(), 1, "retry must not lose the connection");

        core.on_write_ready(id).unwrap();
        assert!(!core.conn(id).unwrap().is_backpressured());
        assert!(!core.conn(id).unwrap().is_paused());
    }

    #[test]
    fn eof_with_queued_echo_defers_close_until_flush() {
        let mut core = ReactorCore::new(EchoHandler);
        let id = core.insert(Conn::new_for_test(64));
        core.on_read_ready(id, b"hello").unwrap();

        core.on_eof(id);
        assert_eq!(core.len(), 1, "EOF must not drop queued echo output");

        // Calling on_write_ready before the platform has flushed the queue must
        // not close; the test's drain seam stands in for the socket write.
        core.on_write_ready(id).unwrap();
        assert_eq!(core.len(), 1, "must not close before the queue drains");
        assert_eq!(
            core.conn_mut(id).unwrap().take_pending_write(),
            Some(b"hello".to_vec())
        );

        core.on_write_ready(id).unwrap();
        assert_eq!(core.len(), 0);
    }

    #[test]
    fn eof_closes_the_connection_and_reports_on_close() {
        let mut core = ReactorCore::new(FailingHandler::new());
        let id = core.insert(Conn::new_for_test(64));

        core.on_eof(id);

        assert!(core.is_empty());
        assert_eq!(core.handler().closed, vec![id]);
    }

    #[test]
    fn eof_with_carried_output_defers_close_until_the_retry_flushes() {
        let mut core = ReactorCore::new(EchoHandler);
        let id = core.insert(Conn::new_for_test(8));
        core.on_read_ready(id, b"12345").unwrap();
        core.on_read_ready(id, b"67890").unwrap();

        core.on_eof(id);
        assert_eq!(core.len(), 1, "EOF must keep the carried chunk alive");

        // The queued chunk drains; the retry then moves the carry into the
        // queue, so the deferred close still waits.
        core.conn_mut(id).unwrap().take_pending_write();
        core.on_write_ready(id).unwrap();
        assert_eq!(
            core.conn_mut(id).unwrap().take_pending_write(),
            Some(b"67890".to_vec())
        );
        assert_eq!(core.len(), 1, "the retried carry must be flushed first");

        core.on_write_ready(id).unwrap();
        assert!(core.is_empty());
    }

    #[test]
    fn explicit_pause_survives_a_drained_write_queue() {
        let mut core = ReactorCore::new(EchoHandler);
        let id = core.insert(Conn::new_for_test(8));
        {
            let conn = core.conn_mut(id).unwrap();
            conn.enqueue(b"hello").unwrap();
            conn.pause_read();
        }

        core.on_write_ready(id).unwrap();
        assert!(core.conn(id).unwrap().is_paused());

        core.conn_mut(id).unwrap().take_pending_write();
        core.on_write_ready(id).unwrap();
        assert!(
            core.conn(id).unwrap().is_paused(),
            "draining output must not clear an explicit pause"
        );

        core.conn_mut(id).unwrap().resume_read();
        core.on_write_ready(id).unwrap();
        assert!(!core.conn(id).unwrap().is_paused());
    }

    #[test]
    fn stale_ids_are_ignored_by_every_helper() {
        let mut core = ReactorCore::new(FailingHandler::new());
        let id = core.insert(Conn::new_for_test(64));
        core.close(id, "test");

        assert!(core.on_read_ready(id, b"x").is_ok());
        assert!(core.on_accept(id).is_ok());
        assert!(core.on_connect(id).is_ok());
        core.on_write_ready(id).unwrap();
        core.on_eof(id);
        assert!(core.is_empty());
    }

    #[test]
    fn core_owns_the_block_pool_and_timer_heap() {
        let mut core = ReactorCore::new(EchoHandler);
        assert!(core.pool().cap_blocks() > 0);
        assert!(core.timers().next_deadline().is_none());

        let deadline = Instant::now();
        let token = core.timers_mut().insert(
            TimerKey {
                slot: 0,
                generation: 0,
                exchange_seq: 0,
                kind: 1,
            },
            deadline,
        );
        assert_eq!(core.timers().next_deadline(), Some(deadline));

        core.timers_mut().cancel(token);
        assert!(core.timers().next_deadline().is_none());
    }
}
