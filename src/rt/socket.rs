//! Nonblocking owned TCP socket.
//!
//! [`OwnedSocket`] owns a [`TcpStream`] and switches it to nonblocking mode on
//! construction. The reactor takes ownership of accepted and connected sockets
//! through this type, polls `as_raw_fd` (Unix) or `as_raw_socket` (Windows) for
//! readiness, and performs I/O through the [`Read`]/[`Write`] implementations.
//! No operation blocks once the socket has been constructed.

use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};

#[cfg(unix)]
use std::os::fd::{AsRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, RawSocket};

/// A TCP socket owned by the reactor.
#[derive(Debug)]
pub(crate) struct OwnedSocket {
    stream: TcpStream,
}

impl OwnedSocket {
    /// Takes ownership of `stream` and switches it to nonblocking mode.
    pub(crate) fn new(stream: TcpStream) -> io::Result<Self> {
        let socket = Self { stream };
        socket.set_nonblocking(true)?;
        Ok(socket)
    }

    /// Sets or clears nonblocking mode.
    ///
    /// Construction already sets it; this exists for the reactor's explicit
    /// state management and for tests.
    pub(crate) fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.stream.set_nonblocking(nonblocking)
    }

    /// Address of the peer, or an error if the socket is disconnected.
    pub(crate) fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream.peer_addr()
    }

    /// Local address of this end of the connection.
    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream.local_addr()
    }

    /// Sends FIN to the peer, leaving this end readable for a half-close.
    pub(crate) fn shutdown_write(&self) -> io::Result<()> {
        self.stream.shutdown(Shutdown::Write)
    }

    /// Closes the socket immediately, discarding unread data.
    ///
    /// Graceful shutdown is the caller's job: call [`Self::shutdown_write`]
    /// first when the peer must observe a clean end of stream.
    pub(crate) fn close(self) {
        let _ = self.stream.shutdown(Shutdown::Both);
    }

    /// Descriptor the reactor registers with epoll/kqueue.
    #[cfg(unix)]
    pub(crate) fn as_raw_fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }

    /// Handle the reactor associates with its IOCP.
    #[cfg(windows)]
    pub(crate) fn as_raw_socket(&self) -> RawSocket {
        self.stream.as_raw_socket()
    }
}

impl Read for OwnedSocket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buf)
    }
}

impl Write for OwnedSocket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};

    fn socket_pair() -> (OwnedSocket, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (server, _peer) = listener.accept().unwrap();
        (OwnedSocket::new(server).unwrap(), client)
    }

    #[test]
    fn constructed_socket_is_nonblocking() {
        let (mut server, _client) = socket_pair();
        let mut buf = [0u8; 1];
        let err = server.read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    }

    #[test]
    fn addresses_are_reported() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let local = listener.local_addr().unwrap();
        let client = TcpStream::connect(local).unwrap();
        let client_addr = client.local_addr().unwrap();
        let (server, peer) = listener.accept().unwrap();
        let socket = OwnedSocket::new(server).unwrap();

        assert_eq!(socket.local_addr().unwrap(), local);
        assert_eq!(socket.peer_addr().unwrap(), peer);
        assert_eq!(peer, client_addr);
    }

    #[test]
    fn shutdown_write_sends_eof_to_the_peer() {
        let (server, mut client) = socket_pair();
        server.shutdown_write().unwrap();

        let mut buf = [0u8; 1];
        assert_eq!(client.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn data_flows_both_ways_and_close_sends_eof() {
        let (mut server, mut client) = socket_pair();

        server.write_all(b"ping").unwrap();
        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"ping");

        client.write_all(b"pong").unwrap();
        server.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"pong");

        server.close();
        assert_eq!(client.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn set_nonblocking_can_be_toggled() {
        let (mut server, _client) = socket_pair();
        server.set_nonblocking(false).unwrap();
        server.set_nonblocking(true).unwrap();

        let mut buf = [0u8; 1];
        let err = server.read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    }

    #[cfg(unix)]
    #[test]
    fn raw_fd_is_exposed() {
        let (server, _client) = socket_pair();
        assert!(server.as_raw_fd() >= 0);
    }

    #[cfg(windows)]
    #[test]
    fn raw_socket_is_exposed() {
        let (server, _client) = socket_pair();
        assert_ne!(server.as_raw_socket(), 0);
    }
}
