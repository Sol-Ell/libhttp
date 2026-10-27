//! Cross-thread reactor wakeup.
//!
//! The reactor polls [`Wake::raw_fd`]; other threads call [`Wake::wake`]. Linux
//! uses a nonblocking, close-on-exec eventfd; the other Unix platforms use a
//! nonblocking, close-on-exec pipe. Pending wakes coalesce into one readable
//! state, so [`Wake::drain`] reports whether anything was pending and leaves
//! the descriptor empty.
//!
//! The descriptors are owned by [`std::fs::File`], which closes them on drop.
//! `wake` and `drain` are thin wrappers over `write(2)` and `read(2)`; no
//! allocation happens per wake.

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};

/// Cross-thread wake channel for a reactor.
pub(crate) struct Wake {
    /// Pollable descriptor: the eventfd, or the pipe's read end.
    read: File,
    /// Pipe write end. An eventfd is bidirectional, so Linux needs no second
    /// handle.
    #[cfg(not(target_os = "linux"))]
    write: File,
}

impl Wake {
    pub(crate) fn new() -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            // SAFETY: eventfd(2) takes no pointers. A negative return is
            // checked before the descriptor is wrapped or used.
            let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: `fd` is a fresh, uniquely owned descriptor; `File` takes
            // ownership and closes it exactly once on drop.
            Ok(Self {
                read: unsafe { File::from_raw_fd(fd) },
            })
        }

        #[cfg(not(target_os = "linux"))]
        {
            let (read, write) = pipe()?;
            Ok(Self { read, write })
        }
    }

    /// Wakes the reactor. Repeated calls before a drain coalesce.
    pub(crate) fn wake(&self) -> io::Result<()> {
        // eventfd counters are 8 bytes; a pipe needs a single byte.
        #[cfg(target_os = "linux")]
        let result = (&self.read).write_all(&1u64.to_ne_bytes());
        #[cfg(not(target_os = "linux"))]
        let result = (&self.write).write_all(&[1u8]);

        match result {
            Ok(()) => Ok(()),
            // The channel is full, so a wake is already pending and the
            // reactor will observe it; reporting an error here would turn a
            // successful signal into a spurious failure.
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Drains all pending wakes. Returns `true` if at least one was pending.
    pub(crate) fn drain(&mut self) -> io::Result<bool> {
        // An eventfd read consumes 8 bytes, but a pipe may hold many wake
        // bytes; one stack buffer serves both.
        let mut buf = [0u8; 64];
        let mut drained = false;

        loop {
            match (&self.read).read(&mut buf) {
                Ok(0) => return Ok(drained),
                Ok(_) => drained = true,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(drained),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Descriptor the reactor polls for readability.
    #[cfg(unix)]
    pub(crate) fn raw_fd(&self) -> RawFd {
        self.read.as_raw_fd()
    }
}

#[cfg(not(target_os = "linux"))]
fn pipe() -> io::Result<(File, File)> {
    let mut fds = [0 as libc::c_int; 2];

    // SAFETY: `fds` is valid storage for the two descriptors pipe(2) writes.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: pipe(2) succeeded, so both descriptors are fresh and uniquely
    // owned here; each is wrapped exactly once and closed on drop. If a later
    // step fails, dropping both `File`s closes them.
    let read = unsafe { File::from_raw_fd(fds[0]) };
    // SAFETY: as above.
    let write = unsafe { File::from_raw_fd(fds[1]) };

    set_nonblocking_cloexec(&read)?;
    set_nonblocking_cloexec(&write)?;

    Ok((read, write))
}

#[cfg(not(target_os = "linux"))]
fn set_nonblocking_cloexec(file: &File) -> io::Result<()> {
    let fd = file.as_raw_fd();

    // SAFETY: fcntl(2) operates on a live descriptor owned by `file`; it takes
    // no pointers and does not retain anything.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: as above.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: as above.
    let fd_flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if fd_flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: as above.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, fd_flags | libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn wake_then_drain() {
        let mut w = Wake::new().unwrap();
        w.wake().unwrap();
        assert!(w.drain().unwrap());
        assert!(!w.drain().unwrap());
    }

    #[test]
    fn many_wakes_coalesce() {
        let mut w = Wake::new().unwrap();
        for _ in 0..64 {
            w.wake().unwrap();
        }
        assert!(w.drain().unwrap());
        assert!(!w.drain().unwrap());
    }

    #[test]
    fn wake_before_poll_is_seen() {
        let mut w = Wake::new().unwrap();

        // The brief's raw-fd write cannot work on either backend: eventfd
        // requires an 8-byte value, and a pipe's pollable fd is its read end.
        // A cross-thread `wake` is the behavior the reactor depends on.
        thread::scope(|scope| {
            scope.spawn(|| w.wake().unwrap());
        });

        assert!(w.drain().unwrap());
    }
}
