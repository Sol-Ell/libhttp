//! Linux edge-triggered epoll backend.
//!
//! The poller owns one `epoll_create1(EPOLL_CLOEXEC)` instance. Every added or
//! modified descriptor is armed with `EPOLLET | EPOLLRDHUP` plus the
//! requested read/write bits, so the reactor must drain readable sockets to
//! `EAGAIN` and keep write interest armed until the outbound queue empties.
//!
//! The registration payload is the [`ConnId`] encoded into the event's `u64`
//! data field. The poller never interprets the id; the reactor may register
//! listener and wakeup descriptors under sentinel ids that the slab cannot
//! produce.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::time::Duration;

use crate::rt::poller::{Interest, PollEvent, Poller};
use crate::rt::slab::ConnId;

/// Number of events fetched from the kernel per `epoll_wait` call.
const MAX_EVENTS: usize = 256;

/// Edge-triggered epoll instance.
pub(crate) struct EpollPoller {
    /// Owns the epoll descriptor; dropping it closes the instance.
    fd: OwnedFd,
}

impl EpollPoller {
    /// Creates an epoll instance with close-on-exec set.
    pub(crate) fn new() -> io::Result<Self> {
        // SAFETY: epoll_create1(2) takes no pointers and a negative return is
        // checked before the descriptor is wrapped.
        let fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fd` is a fresh, uniquely owned descriptor; `OwnedFd` takes
        // ownership and closes it exactly once on drop.
        Ok(Self {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }

    /// Runs one `epoll_ctl` operation.
    ///
    /// `event` is required for add/modify and is ignored (null is passed) for
    /// delete.
    fn control(
        &self,
        op: libc::c_int,
        fd: RawFd,
        event: Option<&mut libc::epoll_event>,
    ) -> io::Result<()> {
        let event = event.map_or(std::ptr::null_mut(), |event| {
            event as *mut libc::epoll_event
        });
        // SAFETY: the epoll descriptor is live and owned by `self`; `fd` is a
        // live descriptor supplied by the reactor; `event`, when non-null,
        // points at a valid `epoll_event` for the duration of the call and
        // epoll_ctl copies it before returning.
        let result = unsafe { libc::epoll_ctl(self.fd.as_raw_fd(), op, fd, event) };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

/// Translates an [`Interest`] into edge-triggered epoll flags.
fn flags(interest: Interest) -> u32 {
    let readiness = match interest {
        Interest::Read => libc::EPOLLIN,
        Interest::Write => libc::EPOLLOUT,
        Interest::Both => libc::EPOLLIN | libc::EPOLLOUT,
    };
    (readiness | libc::EPOLLRDHUP | libc::EPOLLET) as u32
}

/// Packs a generation-checked id into the event data field.
fn encode(id: ConnId) -> u64 {
    (u64::from(id.generation) << 32) | u64::from(id.slot)
}

/// Unpacks the event data field back into a [`ConnId`].
fn decode(raw: u64) -> ConnId {
    ConnId {
        slot: raw as u32,
        generation: (raw >> 32) as u32,
    }
}

impl Poller for EpollPoller {
    fn add(&mut self, fd: RawFd, id: ConnId, interest: Interest) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: flags(interest),
            u64: encode(id),
        };
        self.control(libc::EPOLL_CTL_ADD, fd, Some(&mut event))
    }

    fn modify(&mut self, fd: RawFd, id: ConnId, interest: Interest) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: flags(interest),
            u64: encode(id),
        };
        self.control(libc::EPOLL_CTL_MOD, fd, Some(&mut event))
    }

    fn remove(&mut self, fd: RawFd) -> io::Result<()> {
        self.control(libc::EPOLL_CTL_DEL, fd, None)
    }

    fn wait(&mut self, timeout: Option<Duration>, events: &mut Vec<PollEvent>) -> io::Result<()> {
        events.clear();

        // `epoll_wait` takes milliseconds; `None` means block indefinitely.
        let timeout_ms = match timeout {
            None => -1,
            Some(duration) => duration.as_millis().min(i32::MAX as u128) as i32,
        };

        // The kernel overwrites only the first `count` slots.
        let mut raw = [libc::epoll_event { events: 0, u64: 0 }; MAX_EVENTS];
        // SAFETY: the epoll descriptor is live and owned by `self`; `raw`
        // provides MAX_EVENTS writable slots; epoll_wait writes at most that
        // many entries and returns how many it wrote.
        let count = unsafe {
            libc::epoll_wait(
                self.fd.as_raw_fd(),
                raw.as_mut_ptr(),
                MAX_EVENTS as libc::c_int,
                timeout_ms,
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

        for slot in raw.iter().take(count as usize) {
            // Copy out of the packed kernel struct before inspecting fields.
            let event = *slot;
            let bits = event.events;
            events.push(PollEvent {
                id: decode(event.u64),
                // EPOLLRDHUP and EPOLLHUP are reported as readable so the
                // reactor attempts a final read and observes EOF.
                readable: bits
                    & (libc::EPOLLIN as u32 | libc::EPOLLRDHUP as u32 | libc::EPOLLHUP as u32)
                    != 0,
                writable: bits & libc::EPOLLOUT as u32 != 0,
                error: (bits & libc::EPOLLERR as u32 != 0)
                    .then(|| io::Error::other("epoll reported an error for the connection")),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::EpollPoller;
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
        let mut poller = EpollPoller::new().unwrap();
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
        let mut poller = EpollPoller::new().unwrap();
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
    fn remove_stops_reporting_events() {
        let (reader, mut writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        let mut poller = EpollPoller::new().unwrap();
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
}
