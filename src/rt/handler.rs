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
    /// An error closes the connection. A handler that cannot enqueue the chunk
    /// because the outbound queue is full should keep the bytes (for example
    /// in [`Conn::put_carry`]) and return `Ok(())`; the reactor has already
    /// paused reads. It retries through [`Self::on_output_ready`] once the
    /// queue drains.
    fn on_read(&mut self, conn: &mut Conn, data: &[u8]) -> io::Result<()>;

    /// Called after the outbound queue drains, before reads may resume.
    ///
    /// A handler that carried output after backpressure retries it here. An
    /// error closes the connection; the default does nothing.
    fn on_output_ready(&mut self, _conn: &mut Conn) -> io::Result<()> {
        Ok(())
    }

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
            // The queue refused the chunk. `enqueue` has already set
            // backpressure (which pauses reads), so the stream is complete as
            // long as the chunk is retained for a later retry. Returning
            // `WouldBlock` here would reset the connection and lose it.
            Err(WriteError::QueueFull) | Err(WriteError::PoolExhausted) => {
                conn.put_carry(data.to_vec());
                Ok(())
            }
            Err(WriteError::Closed) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "cannot echo to a closed connection",
            )),
        }
    }

    fn on_output_ready(&mut self, conn: &mut Conn) -> io::Result<()> {
        let Some(data) = conn.take_carry() else {
            return Ok(());
        };
        match conn.enqueue(&data) {
            // Still no room (the reactor only retries after a drain, so this is
            // a pool exhaustion or a chunk larger than the cap): keep it for
            // the next drain instead of dropping bytes.
            Err(WriteError::QueueFull) | Err(WriteError::PoolExhausted) => {
                conn.put_carry(data);
                Ok(())
            }
            Err(WriteError::Closed) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "cannot echo to a closed connection",
            )),
            Ok(()) => Ok(()),
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
    fn echo_handler_carries_overflow_instead_of_resetting() {
        let mut h = EchoHandler;
        let mut c = Conn::new_for_test(16);
        h.on_read(&mut c, b"0123456789").unwrap();
        assert!(!c.is_paused());

        // The second chunk does not fit; the handler must carry it and let the
        // reactor pause reads instead of treating the overflow as fatal.
        h.on_read(&mut c, b"abcdefghij").unwrap();
        assert!(c.is_backpressured());
        assert!(!c.is_read_paused());
        assert!(c.has_carry());
        assert_eq!(c.take_pending_write(), Some(b"0123456789".to_vec()));

        h.on_output_ready(&mut c).unwrap();
        assert!(!c.has_carry());
        assert_eq!(c.take_pending_write(), Some(b"abcdefghij".to_vec()));
    }

    #[test]
    fn echo_handler_output_ready_is_a_noop_without_carry() {
        let mut h = EchoHandler;
        let mut c = Conn::new_for_test(64);
        h.on_output_ready(&mut c).unwrap();
        assert!(!c.has_pending_write());
        assert!(!c.has_carry());
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
