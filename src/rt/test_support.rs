//! Test-only loopback harness for the platform reactors.
//!
//! `spawn_echo` starts a reactor on a background thread and returns its bound
//! address, a [`Shutdown`] handle and the thread's join handle. The harness is
//! `cfg(test)` only: M1 has no public runtime API.

#![cfg(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "freebsd",
    windows
))]

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::JoinHandle;

use crate::rt::handler::EchoHandler;
#[cfg(windows)]
use crate::rt::reactor::iocp::IocpReactor;
#[cfg(unix)]
use crate::rt::reactor::unix::UnixReactor;
use crate::rt::wake::Wake;

/// Stops a reactor thread spawned by [`spawn_echo`] or [`spawn_echo_on`].
pub(crate) struct Shutdown {
    wake: Arc<Wake>,
}

impl Shutdown {
    /// Wraps the wake descriptor the reactor polls.
    pub(crate) fn new(wake: Arc<Wake>) -> Self {
        Self { wake }
    }

    /// Requests shutdown. Safe to call more than once; wakes coalesce.
    pub(crate) fn shutdown(&self) {
        let _ = self.wake.wake();
    }
}

/// Starts an echo reactor on an ephemeral `127.0.0.1` port.
pub(crate) fn spawn_echo() -> (SocketAddr, Shutdown, JoinHandle<io::Result<()>>) {
    #[cfg(unix)]
    {
        UnixReactor::<EchoHandler>::spawn_for_test()
    }
    #[cfg(windows)]
    {
        IocpReactor::<EchoHandler>::spawn_for_test()
    }
}

/// Starts an echo reactor bound to `addr`.
pub(crate) fn spawn_echo_on(
    addr: &str,
) -> io::Result<(SocketAddr, Shutdown, JoinHandle<io::Result<()>>)> {
    #[cfg(unix)]
    {
        UnixReactor::<EchoHandler>::new(EchoHandler).spawn_for_test_on(addr)
    }
    #[cfg(windows)]
    {
        IocpReactor::<EchoHandler>::new(EchoHandler).spawn_for_test_on(addr)
    }
}
