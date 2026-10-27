//! macOS/FreeBSD edge-triggered kqueue backend.
//!
//! The poller owns one `kqueue()` instance. Every added or modified descriptor
//! is registered with both `EVFILT_READ` and `EVFILT_WRITE` under
//! `EV_ADD | EV_CLEAR`; the filters that the current [`Interest`] does not ask
//! for are kept disabled. Re-adding an existing filter with `EV_ADD` modifies
//! it in place, so `add` and `modify` share one path and the backend never has
//! to reconstruct kernel state or delete a filter that was not armed.
//!
//! `EV_CLEAR` makes the filters edge-triggered, so the reactor must drain
//! readable sockets to `EAGAIN` and keep write interest armed until the
//! outbound queue empties.
//!
//! The [`ConnId`] travels in `kevent::udata`; the kernel returns it unchanged.
//! The identifier is 64 bits and the backend only supports 64-bit targets
//! (macOS and FreeBSD on x86-64/arm64), which is asserted at compile time. The
//! reactor may register listener and wakeup descriptors under sentinel ids
//! that the slab cannot produce.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::time::Duration;

use crate::rt::poller::{Interest, PollEvent, Poller};
use crate::rt::slab::ConnId;

/// Number of events fetched from the kernel per `kevent` call.
const MAX_EVENTS: usize = 256;

/// Longest timeout `kevent` accepts on macOS; longer values return `EINVAL`.
const MAX_TIMEOUT_SECS: u64 = 1_000_000;

// `ConnId` is carried through `udata`, so a 64-bit pointer is required.
#[cfg(not(target_pointer_width = "64"))]
compile_error!("kqueue poller requires a 64-bit target (ConnId in kevent.udata)");

/// Edge-triggered kqueue instance.
pub(crate) struct KqueuePoller {
    /// Owns the kqueue descriptor; dropping it closes the instance.
    fd: OwnedFd,
}

impl KqueuePoller {
    /// Creates a kqueue instance with close-on-exec set.
    pub(crate) fn new() -> io::Result<Self> {
        // SAFETY: kqueue(2) takes no pointers; a negative return is checked
        // before the descriptor is wrapped.
        let fd = unsafe { libc::kqueue() };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fd` is a fresh, uniquely owned descriptor; `OwnedFd` takes
        // ownership and closes it exactly once on drop.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        set_cloexec(fd.as_raw_fd())?;
        Ok(Self { fd })
    }

    /// Applies a changelist without retrieving readiness events.
    ///
    /// `nevents = 0` means a failed change is reported by `kevent` returning
    /// `-1` instead of by an `EV_ERROR` entry, so no readiness edge can be
    /// consumed and discarded while interest is being updated.
    fn apply(&self, changes: &[libc::kevent]) -> io::Result<()> {
        debug_assert!(
            !changes.is_empty(),
            "a changelist needs at least one change"
        );

        // A zero timeout applies the changes immediately; there are no events
        // to wait for because the eventlist is empty.
        let timeout = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };

        // SAFETY: the kqueue descriptor is live and owned by `self`; `changes`
        // points at a valid array of `changes.len()` initialized entries; the
        // null eventlist is permitted with `nevents = 0`; `timeout` is a valid
        // timespec for the duration of the call. kevent(2) copies the
        // changelist and does not retain any pointer.
        let result = unsafe {
            libc::kevent(
                self.fd.as_raw_fd(),
                changes.as_ptr(),
                changes.len() as libc::c_int,
                std::ptr::null_mut(),
                0,
                &timeout,
            )
        };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    /// Adds or re-arms both filters for `fd` under `id`.
    ///
    /// Re-adding an existing filter changes it in place, so this is also the
    /// modify path. A filter outside `interest` is registered disabled rather
    /// than deleted, which keeps the kernel state valid for a later re-enable.
    fn arm(&self, fd: RawFd, id: ConnId, interest: Interest) -> io::Result<()> {
        let (read, write) = match interest {
            Interest::Read => (true, false),
            Interest::Write => (false, true),
            Interest::Both => (true, true),
        };

        let changes = [
            arm_change(fd, id, libc::EVFILT_READ, read),
            arm_change(fd, id, libc::EVFILT_WRITE, write),
        ];
        self.apply(&changes)
    }
}

/// Sets `FD_CLOEXEC` on a fresh descriptor.
fn set_cloexec(fd: RawFd) -> io::Result<()> {
    // SAFETY: fcntl(2) operates on a live descriptor owned by the caller; it
    // takes no pointers and does not retain anything.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: as above.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Builds an `EV_ADD | EV_CLEAR` change that enables or disables one filter.
fn arm_change(fd: RawFd, id: ConnId, filter: i16, enable: bool) -> libc::kevent {
    let mut flags = libc::EV_ADD | libc::EV_CLEAR;
    flags |= if enable {
        libc::EV_ENABLE
    } else {
        libc::EV_DISABLE
    };

    let mut event = empty_event();
    event.ident = fd as libc::uintptr_t;
    event.filter = filter;
    event.flags = flags;
    event.udata = encode(id);
    event
}

/// Builds an `EV_DELETE` change for one filter.
fn delete_change(fd: RawFd, filter: i16) -> libc::kevent {
    let mut event = empty_event();
    event.ident = fd as libc::uintptr_t;
    event.filter = filter;
    event.flags = libc::EV_DELETE;
    event
}

/// A zeroed `kevent` with the fields this backend does not use left at zero.
fn empty_event() -> libc::kevent {
    // SAFETY: `kevent` is a plain C data structure (integer fields, a pointer
    // and, on FreeBSD, a fixed-size array). All-zero is a valid representation
    // and no field has a niche or invalid zero value.
    unsafe { std::mem::zeroed() }
}

/// Packs a generation-checked id into `kevent::udata`.
fn encode(id: ConnId) -> *mut libc::c_void {
    ((u64::from(id.generation) << 32) | u64::from(id.slot)) as usize as *mut libc::c_void
}

/// Unpacks `kevent::udata` back into a [`ConnId`].
fn decode(udata: *mut libc::c_void) -> ConnId {
    let raw = udata as usize as u64;
    ConnId {
        slot: raw as u32,
        generation: (raw >> 32) as u32,
    }
}

impl Poller for KqueuePoller {
    fn add(&mut self, fd: RawFd, id: ConnId, interest: Interest) -> io::Result<()> {
        self.arm(fd, id, interest)
    }

    fn modify(&mut self, fd: RawFd, id: ConnId, interest: Interest) -> io::Result<()> {
        self.arm(fd, id, interest)
    }

    fn remove(&mut self, fd: RawFd) -> io::Result<()> {
        let changes = [
            delete_change(fd, libc::EVFILT_READ),
            delete_change(fd, libc::EVFILT_WRITE),
        ];
        self.apply(&changes)
    }

    fn wait(&mut self, timeout: Option<Duration>, events: &mut Vec<PollEvent>) -> io::Result<()> {
        events.clear();

        let timeout = timeout.map(|duration| libc::timespec {
            tv_sec: libc::time_t::try_from(duration.as_secs().min(MAX_TIMEOUT_SECS))
                .unwrap_or(libc::time_t::MAX),
            tv_nsec: duration.subsec_nanos() as libc::c_long,
        });
        let timeout = timeout
            .as_ref()
            .map_or(std::ptr::null(), |timeout| timeout as *const libc::timespec);

        // The kernel overwrites only the first `count` slots.
        let mut raw: [libc::kevent; MAX_EVENTS] = [empty_event(); MAX_EVENTS];
        // SAFETY: the kqueue descriptor is live and owned by `self`; `raw`
        // provides MAX_EVENTS writable slots; kevent writes at most that many
        // entries and returns how many it wrote; `timeout` is either null
        // (block forever) or points at a live timespec for the call.
        let count = unsafe {
            libc::kevent(
                self.fd.as_raw_fd(),
                std::ptr::null(),
                0,
                raw.as_mut_ptr(),
                MAX_EVENTS as libc::c_int,
                timeout,
            )
        };
        if count < 0 {
            let err = io::Error::last_os_error();
            // A signal is not a polling failure; the reactor loop re-enters
            // and recomputes its timeout.
            if err.kind() == io::ErrorKind::Interrupted {
                return Ok(());
            }
            return Err(err);
        }

        for event in raw.iter().take(count as usize) {
            // Copy out of the kernel struct before inspecting fields.
            let event = *event;
            let id = decode(event.udata);
            let flags = event.flags;

            if flags & libc::EV_ERROR != 0 {
                events.push(PollEvent {
                    id,
                    readable: false,
                    writable: false,
                    error: Some(io::Error::from_raw_os_error(event.data as i32)),
                });
                continue;
            }

            // Both filters run edge-triggered. A read filter event, or an EOF
            // reported by either filter, is surfaced as readable so the
            // reactor attempts a final read and observes the end of stream.
            let readable = event.filter == libc::EVFILT_READ || flags & libc::EV_EOF != 0;
            let writable = event.filter == libc::EVFILT_WRITE;

            if !readable && !writable {
                continue;
            }
            events.push(PollEvent {
                id,
                readable,
                writable,
                error: None,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::KqueuePoller;
    use crate::rt::poller::{Interest, Poller};
    use crate::rt::slab::ConnId;
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    fn id(slot: u32, generation: u32) -> ConnId {
        ConnId { slot, generation }
    }

    #[test]
    fn wait_reports_readable_data_and_its_conn_id() {
        let (mut reader, mut writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        let mut poller = KqueuePoller::new().unwrap();
        let conn = id(7, 3);
        poller
            .add(reader.as_raw_fd(), conn, Interest::Read)
            .unwrap();

        let mut events = Vec::new();
        poller
            .wait(Some(Duration::from_millis(5)), &mut events)
            .unwrap();
        assert!(events.is_empty(), "no data yet");

        writer.write_all(b"x").unwrap();
        poller
            .wait(Some(Duration::from_millis(100)), &mut events)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, conn);
        assert!(events[0].readable);
        assert!(!events[0].writable);
        assert!(events[0].error.is_none());

        let mut buf = [0u8; 1];
        assert_eq!(reader.read(&mut buf).unwrap(), 1);
        poller
            .wait(Some(Duration::from_millis(5)), &mut events)
            .unwrap();
        assert!(
            events.is_empty(),
            "edge-triggered fd stays quiet after drain"
        );
    }

    #[test]
    fn modify_switches_read_interest_to_write_interest() {
        let (reader, _writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        let mut poller = KqueuePoller::new().unwrap();
        let conn = id(1, 2);
        poller
            .add(reader.as_raw_fd(), conn, Interest::Read)
            .unwrap();
        poller
            .modify(reader.as_raw_fd(), conn, Interest::Write)
            .unwrap();

        let mut events = Vec::new();
        poller
            .wait(Some(Duration::from_millis(100)), &mut events)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, conn);
        assert!(events[0].writable);
        assert!(!events[0].readable);
    }

    #[test]
    fn both_interest_arms_read_and_write() {
        let (reader, mut writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        let mut poller = KqueuePoller::new().unwrap();
        let conn = id(5, 6);
        poller
            .add(reader.as_raw_fd(), conn, Interest::Both)
            .unwrap();

        let mut events = Vec::new();
        poller
            .wait(Some(Duration::from_millis(100)), &mut events)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, conn);
        assert!(events[0].writable);
        assert!(!events[0].readable);

        // The writable edge was consumed; bytes arriving now must produce a
        // separate readable edge.
        writer.write_all(b"x").unwrap();
        poller
            .wait(Some(Duration::from_millis(100)), &mut events)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, conn);
        assert!(events[0].readable);
    }

    #[test]
    fn remove_stops_reporting_events() {
        let (reader, mut writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        let mut poller = KqueuePoller::new().unwrap();
        poller
            .add(reader.as_raw_fd(), id(4, 4), Interest::Read)
            .unwrap();
        poller.remove(reader.as_raw_fd()).unwrap();

        writer.write_all(b"x").unwrap();
        let mut events = Vec::new();
        poller
            .wait(Some(Duration::from_millis(20)), &mut events)
            .unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn re_adding_read_interest_reports_pending_data() {
        let (reader, mut writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        let mut poller = KqueuePoller::new().unwrap();
        let conn = id(9, 11);
        poller
            .add(reader.as_raw_fd(), conn, Interest::Read)
            .unwrap();
        poller.remove(reader.as_raw_fd()).unwrap();

        // The byte arrives while the descriptor has no kernel registration.
        writer.write_all(b"x").unwrap();

        poller
            .add(reader.as_raw_fd(), conn, Interest::Read)
            .unwrap();
        let mut events = Vec::new();
        poller
            .wait(Some(Duration::from_millis(100)), &mut events)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, conn);
        assert!(
            events[0].readable,
            "pending data must produce an edge on re-add"
        );
        assert!(!events[0].writable);
    }

    #[test]
    fn re_enabling_both_after_write_interest_reports_pending_data() {
        let (reader, mut writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        let mut poller = KqueuePoller::new().unwrap();
        let conn = id(10, 12);
        poller
            .add(reader.as_raw_fd(), conn, Interest::Read)
            .unwrap();
        poller
            .modify(reader.as_raw_fd(), conn, Interest::Write)
            .unwrap();

        // The byte arrives while read interest is disabled.
        writer.write_all(b"x").unwrap();

        poller
            .modify(reader.as_raw_fd(), conn, Interest::Both)
            .unwrap();
        let mut events = Vec::new();
        poller
            .wait(Some(Duration::from_millis(100)), &mut events)
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| event.id == conn && event.readable),
            "pending data must produce a readable edge when Both is re-armed"
        );
    }

    #[test]
    fn peer_close_is_reported_readable() {
        let (reader, writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        let mut poller = KqueuePoller::new().unwrap();
        let conn = id(2, 9);
        poller
            .add(reader.as_raw_fd(), conn, Interest::Read)
            .unwrap();
        drop(writer);

        let mut events = Vec::new();
        poller
            .wait(Some(Duration::from_millis(100)), &mut events)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, conn);
        assert!(events[0].readable, "EOF must make the reactor read");
        assert!(events[0].error.is_none());
    }
}
