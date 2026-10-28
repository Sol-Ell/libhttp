//! Small platform shims over nonblocking `mio` sockets.

use std::io::{self, Read};

use mio::net::TcpStream;

/// Applies the default socket options.
pub(crate) fn configure(stream: &TcpStream) -> io::Result<()> {
    let socket = socket2::SockRef::from(stream);
    socket.set_tcp_nodelay(true)?;
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "visionos"
    ))]
    {
        socket.set_nosigpipe(true)?;
    }
    Ok(())
}

/// Reads from a nonblocking socket.
pub(crate) fn read(stream: &mut TcpStream, buffer: &mut [u8]) -> io::Result<usize> {
    stream.read(buffer)
}

/// Writes to a nonblocking socket without raising `SIGPIPE`.
pub(crate) fn send(stream: &TcpStream, buffer: &[u8]) -> io::Result<usize> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        let flags = if cfg!(target_os = "linux") {
            libc::MSG_NOSIGNAL
        } else {
            0
        };
        let written = unsafe {
            libc::send(
                stream.as_raw_fd(),
                buffer.as_ptr().cast(),
                buffer.len(),
                flags,
            )
        };
        if written < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(written as usize)
        }
    }
    #[cfg(not(unix))]
    {
        use std::io::Write;
        let mut stream = stream;
        stream.write(buffer)
    }
}

/// Whether an I/O error means "try again later".
pub(crate) fn is_would_block(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock || error.kind() == io::ErrorKind::TimedOut
}
