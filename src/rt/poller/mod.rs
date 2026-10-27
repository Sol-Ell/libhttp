//! Readiness-poller seam shared by the Unix reactors.
//!
//! A [`Poller`] maps an OS descriptor to the generation-checked [`ConnId`] the
//! reactor uses for its slab entry, then reports readiness as [`PollEvent`]s.
//! The poller is deliberately thin: it performs no socket I/O and makes no
//! connection-lifecycle decisions. The reactor owns both.
//!
//! Every backend registers descriptors edge-triggered and arms only the
//! interests the reactor asks for. A readable event must therefore be drained
//! to `EAGAIN` by the caller; the kernel will not report the same edge twice.

#[cfg(unix)]
use std::io;
#[cfg(unix)]
use std::os::fd::RawFd;
#[cfg(unix)]
use std::time::Duration;

#[cfg(unix)]
use crate::rt::slab::ConnId;

/// Which readiness edges a registration asks the poller to report.
#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Interest {
    /// Report readable edges only.
    Read,
    /// Report writable edges only.
    Write,
    /// Report readable and writable edges.
    Both,
}

/// One readiness notification for a registered descriptor.
#[cfg(unix)]
pub(crate) struct PollEvent {
    /// The reactor id supplied when the descriptor was registered.
    pub(crate) id: ConnId,
    /// Data (or EOF/hangup) can be read without blocking.
    pub(crate) readable: bool,
    /// At least part of a pending write can be sent without blocking.
    pub(crate) writable: bool,
    /// The backend reported a terminal error for the descriptor.
    pub(crate) error: Option<io::Error>,
}

/// Native readiness backend.
#[cfg(unix)]
pub(crate) trait Poller {
    /// Registers `fd` under `id` with `interest` and the backend's edge mode.
    fn add(&mut self, fd: RawFd, id: ConnId, interest: Interest) -> io::Result<()>;

    /// Replaces the interest (and id) of an already-registered `fd`.
    fn modify(&mut self, fd: RawFd, id: ConnId, interest: Interest) -> io::Result<()>;

    /// Unregisters `fd`. Removing an unregistered descriptor is an error.
    fn remove(&mut self, fd: RawFd) -> io::Result<()>;

    /// Waits up to `timeout` (`None` waits indefinitely) and replaces the
    /// contents of `events` with the ready descriptors.
    fn wait(&mut self, timeout: Option<Duration>, events: &mut Vec<PollEvent>) -> io::Result<()>;
}

#[cfg(target_os = "linux")]
pub(crate) mod epoll;
#[cfg(not(target_os = "linux"))]
pub(crate) mod kqueue;
