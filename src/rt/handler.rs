//! Callback seam between the platform reactors and byte-stream consumers.
//!
//! The owning reactor calls these methods on its own thread. A callback that
//! returns an error is terminal for that connection: [`ReactorCore`] closes the
//! connection and reports it through [`Handler::on_close`]. Handlers see only
//! plaintext bytes and connection state, never a socket or a reactor.
//!
//! [`ReactorCore`]: super::reactor::ReactorCore

use std::io;

use super::conn::{Conn, WriteError};
use super::slab::ConnId;

/// Receives connection lifecycle events from a reactor.
pub(crate) trait Handler {
    /// Called once for an accepted connection, before its first read.
    ///
    /// An error closes the connection before it ever reads.
    fn on_accept(&mut self, conn: &mut Conn) -> io::Result<()>;

    /// Called once for an outbound connection after it is established.
    ///
    /// The default does nothing; client handlers override it to start a
    /// request. An error closes the connection.
    fn on_connect(&mut self, _conn: &mut Conn) -> io::Result<()> {
        Ok(())
    }

    /// Called with each plaintext chunk read from `conn`.
    ///
    /// An error closes the connection.
    fn on_read(&mut self, conn: &mut Conn, data: &[u8]) -> io::Result<()>;

    /// Called once when the connection closes.
    ///
    /// `id` is already removed from the slab, so it can be recorded or compared
    /// but not used to look the connection up. The connection's socket, queued
    /// output and blocks have been released by the time this is called.
    fn on_close(&mut self, id: ConnId);
}

/// The M1 handler: echoes every plaintext chunk back to the peer.
///
/// `on_accept` and `on_close` have no state to manage, so they do nothing.
pub(crate) struct EchoHandler;

impl Handler for EchoHandler {
    fn on_accept(&mut self, _conn: &mut Conn) -> io::Result<()> {
        Ok(())
    }

    fn on_read(&mut self, conn: &mut Conn, data: &[u8]) -> io::Result<()> {
        match conn.enqueue(data) {
            Ok(()) => Ok(()),
            // Backpressure must not silently truncate the stream. `enqueue` has
            // already paused reads; returning `WouldBlock` makes the reactor
            // close/reset instead of losing this chunk. Task 10 replaces this
            // with carry/retry backpressure.
            Err(WriteError::QueueFull) | Err(WriteError::PoolExhausted) => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "echo output queue cannot accept the chunk",
            )),
            Err(WriteError::Closed) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "cannot echo to a closed connection",
            )),
        }
    }

    fn on_close(&mut self, _id: ConnId) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rt::conn::Conn;
    use crate::rt::slab::ConnId;

    #[test]
    fn echo_handler_writes_back_exactly_the_input() {
        let mut h = EchoHandler;
        let mut c = Conn::new_for_test(64);
        h.on_read(&mut c, b"hello").unwrap();
        assert_eq!(c.take_pending_write(), Some(b"hello".to_vec()));
    }

    #[test]
    fn echo_handler_errors_when_queue_full() {
        let mut h = EchoHandler;
        let mut c = Conn::new_for_test(4);
        let err = h.on_read(&mut c, b"hello").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
        assert!(c.is_paused());
    }

    #[test]
    fn echo_handler_preserves_chunk_order() {
        let mut h = EchoHandler;
        let mut c = Conn::new_for_test(64);
        h.on_read(&mut c, b"ab").unwrap();
        h.on_read(&mut c, b"cd").unwrap();
        assert_eq!(c.take_pending_write(), Some(b"abcd".to_vec()));
    }

    #[test]
    fn echo_handler_accept_and_close_are_no_ops() {
        let mut h = EchoHandler;
        let mut c = Conn::new_for_test(64);
        h.on_accept(&mut c).unwrap();
        h.on_connect(&mut c).unwrap();
        h.on_close(ConnId {
            slot: 0,
            generation: 0,
        });
        assert!(!c.has_pending_write());
        assert!(!c.is_closed());
    }

    #[test]
    fn echo_handler_rejects_writes_on_a_closed_connection() {
        let mut h = EchoHandler;
        let mut c = Conn::new_for_test(64);
        c.close();
        let err = h.on_read(&mut c, b"x").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
    }
}
