//! Windows I/O completion port and overlapped-socket helpers.
//!
//! This is the completion-backend counterpart of the Unix readiness pollers.
//! It owns the port handle, associates sockets with it, posts the wake sentinel
//! and dequeues completions in bounded batches. Socket setup that is specific to
//! overlapped I/O (`AcceptEx`/`ConnectEx` contexts, wildcard binds and the
//! `ConnectEx` extension pointer) also lives here, so the reactor can stay
//! focused on connection lifecycle.
//!
//! Every operation the reactor posts owns a heap `OVERLAPPED`; the reactor keeps
//! that owner until the matching completion is dequeued. The port never touches
//! freed memory because a completion is the only thing that releases the box.

#![cfg(windows)]

use std::ffi::c_void;
use std::io;
use std::mem::size_of;
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::Duration;

use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE, WAIT_TIMEOUT};
use windows_sys::Win32::Networking::WinSock::{
    ADDRESS_FAMILY, AF_INET, AF_INET6, INVALID_SOCKET, IPPROTO_TCP,
    SIO_GET_EXTENSION_FUNCTION_POINTER, SO_UPDATE_ACCEPT_CONTEXT, SO_UPDATE_CONNECT_CONTEXT,
    SOCK_STREAM, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKET, SOCKET_ERROR, SOL_SOCKET,
    WSA_FLAG_OVERLAPPED, WSADATA, WSAGetLastError, WSAID_CONNECTEX, WSAIoctl, WSASocketW,
    WSAStartup, bind, closesocket, htons, setsockopt,
};
use windows_sys::Win32::System::IO::{
    CancelIoEx, CreateIoCompletionPort, GetOverlappedResult, GetQueuedCompletionStatusEx,
    OVERLAPPED, OVERLAPPED_ENTRY,
};

/// Completion key for the listening socket's `AcceptEx` completions.
pub(crate) const LISTEN_KEY: usize = 1;
/// Completion key for connection sockets.
pub(crate) const CONN_KEY: usize = 2;
/// Completion key for the reactor wake sentinel.
pub(crate) const WAKE_KEY: usize = 3;

/// Completions dequeued in one `GetQueuedCompletionStatusEx` call.
pub(crate) const MAX_COMPLETIONS: usize = 64;

/// The `ConnectEx` extension function, loaded with `WSAIoctl`.
///
/// `windows-sys` does not link `ConnectEx`, so the pointer has to be resolved
/// per provider at runtime.
pub(crate) type ConnectExFn = unsafe extern "system" fn(
    socket: SOCKET,
    name: *const SOCKADDR,
    namelen: i32,
    send_buffer: *const c_void,
    send_data_length: u32,
    bytes_sent: *mut u32,
    overlapped: *mut OVERLAPPED,
) -> i32;

/// Owned I/O completion port.
pub(crate) struct Iocp {
    port: HANDLE,
}

// SAFETY: a completion-port handle is process-wide and may be used from any
// thread; Windows serializes concurrent `GetQueuedCompletionStatusEx` and
// `PostQueuedCompletionStatus` calls. `Iocp` owns the handle, so moving it to
// the reactor thread transfers that ownership without sharing it.
unsafe impl Send for Iocp {}

impl Iocp {
    /// Creates a completion port, initializing Winsock if this is the first use.
    pub(crate) fn new() -> io::Result<Self> {
        ensure_winsock()?;
        // SAFETY: `INVALID_HANDLE_VALUE` asks for a new, unassociated port and
        // the null completion key is required when no file handle is supplied.
        let port =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 0) };
        if port.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { port })
    }

    /// The raw port handle, used by the test harness to build a
    /// [`Wake`](crate::rt::wake::Wake).
    #[cfg(test)]
    pub(crate) fn raw_handle(&self) -> usize {
        self.port as usize
    }

    /// Associates `socket` with this port under `key`.
    pub(crate) fn associate(&self, socket: SOCKET, key: usize) -> io::Result<()> {
        // SAFETY: both handles are live for this call, and the existing port is
        // owned by `self`.
        let added = unsafe { CreateIoCompletionPort(socket as HANDLE, self.port, key, 0) };
        if added.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Waits up to `timeout` (`None` waits indefinitely) for completions.
    ///
    /// Returns the number of entries written to `entries`; zero means the wait
    /// timed out. The returned entries are valid until the next call.
    pub(crate) fn wait(
        &self,
        timeout: Option<Duration>,
        entries: &mut [OVERLAPPED_ENTRY; MAX_COMPLETIONS],
    ) -> io::Result<usize> {
        let mut count = 0u32;
        // SAFETY: `entries` is valid for `MAX_COMPLETIONS` entries, `count` is
        // valid out storage, and the port is live.
        let ok = unsafe {
            GetQueuedCompletionStatusEx(
                self.port,
                entries.as_mut_ptr(),
                entries.len() as u32,
                &mut count,
                timeout_millis(timeout),
                0,
            )
        };
        if ok != 0 {
            return Ok(count as usize);
        }

        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(WAIT_TIMEOUT as i32) {
            Ok(0)
        } else {
            Err(err)
        }
    }
}

impl Drop for Iocp {
    fn drop(&mut self) {
        // SAFETY: `port` is owned by this value and is closed exactly once.
        unsafe {
            CloseHandle(self.port);
        }
    }
}

/// Cancels one outstanding operation on `socket`.
///
/// `ERROR_NOT_FOUND` means the operation already completed (its completion
/// packet is either queued or was dequeued), which is not an error: the caller
/// still waits for the completion before freeing the operation.
pub(crate) fn cancel(socket: SOCKET, overlapped: *const OVERLAPPED) -> io::Result<()> {
    // SAFETY: the socket and overlapped structure are owned by the caller and
    // stay alive until the matching completion is observed.
    let cancelled = unsafe { CancelIoEx(socket as HANDLE, overlapped) };
    if cancelled != 0 {
        return Ok(());
    }

    let err = io::Error::last_os_error();
    let code = err.raw_os_error();
    if code == Some(windows_sys::Win32::Foundation::ERROR_NOT_FOUND as i32) {
        return Ok(());
    }
    Err(err)
}

/// Returns the bytes transferred by a completed operation, or its error.
///
/// The socket must be the handle the operation was issued on (the listener for
/// `AcceptEx`, the connection for `WSARecv`/`WSASend`/`ConnectEx`).
pub(crate) fn overlapped_result(socket: SOCKET, overlapped: *const OVERLAPPED) -> io::Result<u32> {
    let mut bytes = 0u32;
    // SAFETY: the operation has completed, so this call does not block; the
    // overlapped structure is still owned by the caller.
    let ok = unsafe { GetOverlappedResult(socket as HANDLE, overlapped, &mut bytes, 0) };
    if ok != 0 {
        Ok(bytes)
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Creates an overlapped TCP socket for `family`.
pub(crate) fn create_socket(family: ADDRESS_FAMILY) -> io::Result<SOCKET> {
    ensure_winsock()?;
    // SAFETY: all pointers are null (no protocol info), and the returned socket
    // is checked before use.
    let socket = unsafe {
        WSASocketW(
            family as i32,
            SOCK_STREAM,
            IPPROTO_TCP,
            std::ptr::null(),
            0,
            WSA_FLAG_OVERLAPPED,
        )
    };
    if socket == INVALID_SOCKET {
        return Err(wsa_error());
    }
    Ok(socket)
}

/// Closes a raw socket, ignoring errors.
pub(crate) fn close_socket(socket: SOCKET) {
    if socket != INVALID_SOCKET {
        // SAFETY: `socket` is owned by the caller and is closed exactly once.
        let _ = unsafe { closesocket(socket) };
    }
}

/// Binds `socket` to the wildcard address of its family.
///
/// `ConnectEx` requires a bound socket before the connect is posted.
pub(crate) fn bind_wildcard(socket: SOCKET, family: ADDRESS_FAMILY) -> io::Result<()> {
    let address = SockAddrBuf::unspecified(family);
    // SAFETY: `address` points at a fully initialized sockaddr of the length
    // reported by `len`.
    let rc = unsafe { bind(socket, address.as_ptr(), address.len()) };
    if rc == SOCKET_ERROR {
        return Err(wsa_error());
    }
    Ok(())
}

/// Applies `SO_UPDATE_ACCEPT_CONTEXT`, which makes the accepted socket usable.
pub(crate) fn set_accept_context(socket: SOCKET, listener: SOCKET) -> io::Result<()> {
    // SAFETY: the option value points at a live listener handle and the length
    // is exactly one `SOCKET`.
    let rc = unsafe {
        setsockopt(
            socket,
            SOL_SOCKET,
            SO_UPDATE_ACCEPT_CONTEXT,
            (&listener as *const SOCKET).cast::<u8>(),
            size_of::<SOCKET>() as i32,
        )
    };
    if rc == SOCKET_ERROR {
        return Err(wsa_error());
    }
    Ok(())
}

/// Applies `SO_UPDATE_CONNECT_CONTEXT` after a successful `ConnectEx`.
pub(crate) fn set_connect_context(socket: SOCKET) -> io::Result<()> {
    // SAFETY: this option takes no value; both the pointer and length are zero.
    let rc = unsafe {
        setsockopt(
            socket,
            SOL_SOCKET,
            SO_UPDATE_CONNECT_CONTEXT,
            std::ptr::null(),
            0,
        )
    };
    if rc == SOCKET_ERROR {
        return Err(wsa_error());
    }
    Ok(())
}

/// Resolves the provider's `ConnectEx` pointer for `socket`.
pub(crate) fn load_connect_ex(socket: SOCKET) -> io::Result<ConnectExFn> {
    let mut function: *mut c_void = std::ptr::null_mut();
    let mut bytes = 0u32;
    // SAFETY: the input GUID and output pointer are valid for the sizes given,
    // and the socket is live. `WSAIoctl` writes one function pointer.
    let rc = unsafe {
        WSAIoctl(
            socket,
            SIO_GET_EXTENSION_FUNCTION_POINTER,
            (&WSAID_CONNECTEX as *const windows_sys::core::GUID).cast::<c_void>(),
            size_of::<windows_sys::core::GUID>() as u32,
            (&mut function as *mut *mut c_void).cast::<c_void>(),
            size_of::<*mut c_void>() as u32,
            &mut bytes,
            std::ptr::null_mut(),
            None,
        )
    };
    if rc == SOCKET_ERROR {
        return Err(wsa_error());
    }
    if function.is_null() {
        return Err(io::Error::other(
            "WSAIoctl returned a null ConnectEx pointer",
        ));
    }

    // SAFETY: the provider filled `function` with a pointer to its `ConnectEx`
    // implementation, whose documented signature is `ConnectExFn`.
    Ok(unsafe { std::mem::transmute::<*mut c_void, ConnectExFn>(function) })
}

/// A stack sockaddr sized for either supported address family.
pub(crate) enum SockAddrBuf {
    V4(SOCKADDR_IN),
    V6(SOCKADDR_IN6),
}

impl SockAddrBuf {
    /// Builds the wildcard address for `family`.
    pub(crate) fn unspecified(family: ADDRESS_FAMILY) -> Self {
        if family == AF_INET6 {
            Self::V6(SOCKADDR_IN6 {
                sin6_family: AF_INET6,
                ..SOCKADDR_IN6::default()
            })
        } else {
            Self::V4(SOCKADDR_IN {
                sin_family: AF_INET,
                ..SOCKADDR_IN::default()
            })
        }
    }

    /// Builds the concrete address described by `address`.
    pub(crate) fn from_socket_addr(address: SocketAddr) -> Self {
        match address {
            SocketAddr::V4(v4) => {
                let mut out = SOCKADDR_IN {
                    sin_family: AF_INET,
                    ..SOCKADDR_IN::default()
                };
                // SAFETY: `htons` only reorders bytes and writing the address
                // union is the documented way to fill `S_addr`.
                unsafe {
                    out.sin_port = htons(v4.port());
                    out.sin_addr.S_un.S_addr = u32::from_ne_bytes(v4.ip().octets());
                }
                Self::V4(out)
            }
            SocketAddr::V6(v6) => {
                let mut out = SOCKADDR_IN6 {
                    sin6_family: AF_INET6,
                    ..SOCKADDR_IN6::default()
                };
                // SAFETY: `htons` only reorders bytes and writing the address
                // union is the documented way to fill the 16 address bytes.
                unsafe {
                    out.sin6_port = htons(v6.port());
                    out.sin6_addr.u.Byte = v6.ip().octets();
                    out.Anonymous.sin6_scope_id = v6.scope_id();
                }
                Self::V6(out)
            }
        }
    }

    /// Pointer suitable for `bind`/`ConnectEx`.
    pub(crate) fn as_ptr(&self) -> *const SOCKADDR {
        match self {
            Self::V4(address) => std::ptr::from_ref(address).cast::<SOCKADDR>(),
            Self::V6(address) => std::ptr::from_ref(address).cast::<SOCKADDR>(),
        }
    }

    /// Length in bytes of the concrete sockaddr.
    pub(crate) fn len(&self) -> i32 {
        match self {
            Self::V4(_) => size_of::<SOCKADDR_IN>() as i32,
            Self::V6(_) => size_of::<SOCKADDR_IN6>() as i32,
        }
    }
}

fn timeout_millis(timeout: Option<Duration>) -> u32 {
    match timeout {
        None => u32::MAX,
        Some(duration) => duration.as_millis().min(u32::MAX as u128) as u32,
    }
}

fn wsa_error() -> io::Error {
    // SAFETY: Winsock errors are thread-local and reading them has no
    // preconditions.
    io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
}

/// The last Winsock error on this thread, as an [`io::Error`].
pub(crate) fn last_wsa_error() -> io::Error {
    wsa_error()
}

fn ensure_winsock() -> io::Result<()> {
    static INIT: OnceLock<Result<(), i32>> = OnceLock::new();
    let result = *INIT.get_or_init(|| {
        let mut data = WSADATA::default();
        // SAFETY: `data` is valid out storage for the requested version.
        let rc = unsafe { WSAStartup(0x0202, &mut data) };
        if rc == 0 { Ok(()) } else { Err(rc) }
    });
    result.map_err(io::Error::from_raw_os_error)
}
