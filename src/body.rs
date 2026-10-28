#![allow(dead_code)]
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use bytes::Bytes;

use crate::header::HeaderMap;
use crate::{Error, HeaderError, ProtocolError, TimeoutKind};

/// A cloneable terminal outcome stored in a body channel.
#[derive(Clone, Debug)]
pub(crate) enum Terminal {
    Timeout(TimeoutKind),
    Canceled,
    Closed,
    Overloaded,
    BodyTooLarge {
        limit: u64,
    },
    Protocol(ProtocolError),
    InvalidRequest(&'static str),
    Header(HeaderError),
    Io {
        kind: std::io::ErrorKind,
        message: String,
    },
}

impl Terminal {
    pub(crate) fn to_error(&self) -> Error {
        match self {
            Self::Timeout(kind) => Error::Timeout(*kind),
            Self::Canceled => Error::Canceled,
            Self::Closed => Error::Closed,
            Self::Overloaded => Error::Overloaded,
            Self::BodyTooLarge { limit } => Error::BodyTooLarge { limit: *limit },
            Self::Protocol(error) => Error::Protocol(*error),
            Self::InvalidRequest(detail) => Error::InvalidRequest(detail),
            Self::Header(error) => Error::Header(error.clone()),
            Self::Io { kind, message } => Error::Io(std::io::Error::new(*kind, message.clone())),
        }
    }
}

/// A wakeup hook used by a body channel to tell the owning reactor that capacity or data
/// changed.
pub(crate) trait ChannelWaker: Send + Sync {
    fn wake(&self);
}

/// A bounded, single-direction body queue.
///
/// One side pushes (reactor for inbound bodies, application for outbound bodies) and one side
/// consumes. The consumer blocks on a condition variable; the reactor is woken through
/// `ChannelWaker` so it can resume socket work.
pub(crate) struct BodyChannel {
    state: Mutex<ChannelState>,
    cond: Condvar,
    limit: usize,
    pinned: Arc<PinnedBudget>,
    exchange_pinned: Arc<AtomicUsize>,
    waker: Mutex<Option<Arc<dyn ChannelWaker>>>,
}

struct ChannelState {
    chunks: VecDeque<Bytes>,
    bytes: usize,
    eof: bool,
    finished: bool,
    closed: bool,
    terminal: Option<Terminal>,
    read_bytes: u64,
    written_bytes: u64,
}

impl BodyChannel {
    pub(crate) fn inbound(
        limit: usize,
        pinned: Arc<PinnedBudget>,
        exchange_pinned: Arc<AtomicUsize>,
        waker: Arc<dyn ChannelWaker>,
    ) -> Arc<Self> {
        Self::new(limit, pinned, exchange_pinned, Some(waker))
    }

    pub(crate) fn outbound(
        limit: usize,
        pinned: Arc<PinnedBudget>,
        exchange_pinned: Arc<AtomicUsize>,
        waker: Arc<dyn ChannelWaker>,
    ) -> Arc<Self> {
        Self::new(limit, pinned, exchange_pinned, Some(waker))
    }

    fn new(
        limit: usize,
        pinned: Arc<PinnedBudget>,
        exchange_pinned: Arc<AtomicUsize>,
        waker: Option<Arc<dyn ChannelWaker>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ChannelState {
                chunks: VecDeque::new(),
                bytes: 0,
                eof: false,
                finished: false,
                closed: false,
                terminal: None,
                read_bytes: 0,
                written_bytes: 0,
            }),
            cond: Condvar::new(),
            limit: limit.max(1),
            pinned,
            exchange_pinned,
            waker: Mutex::new(waker),
        })
    }

    pub(crate) fn set_waker(&self, waker: Arc<dyn ChannelWaker>) {
        *self.waker.lock().unwrap() = Some(waker);
    }

    /// Pushes bytes if they fit; returns false when the channel is full or terminal.
    pub(crate) fn push(&self, bytes: Bytes) -> bool {
        if bytes.is_empty() {
            return true;
        }
        let mut state = self.state.lock().unwrap();
        if state.terminal.is_some() || state.closed || state.finished {
            return false;
        }
        if state.bytes.saturating_add(bytes.len()) > self.limit {
            return false;
        }
        state.bytes += bytes.len();
        state.written_bytes = state.written_bytes.saturating_add(bytes.len() as u64);
        state.chunks.push_back(bytes);
        drop(state);
        self.cond.notify_all();
        self.wake();
        true
    }

    /// Pushes a static slice without copying, if it fits.
    pub(crate) fn push_static(&self, bytes: &'static [u8]) -> bool {
        if bytes.is_empty() {
            return true;
        }
        let mut state = self.state.lock().unwrap();
        if state.terminal.is_some() || state.closed || state.finished {
            return false;
        }
        if state.bytes.saturating_add(bytes.len()) > self.limit {
            return false;
        }
        state.bytes += bytes.len();
        state.written_bytes = state.written_bytes.saturating_add(bytes.len() as u64);
        state.chunks.push_back(Bytes::from_static(bytes));
        drop(state);
        self.cond.notify_all();
        self.wake();
        true
    }

    /// Returns the next outbound chunk, if any, for the reactor to write.
    pub(crate) fn take(&self) -> Option<Bytes> {
        let mut state = self.state.lock().unwrap();
        let bytes = state.chunks.pop_front()?;
        state.bytes -= bytes.len();
        drop(state);
        self.cond.notify_all();
        self.wake();
        Some(bytes)
    }

    pub(crate) fn read(&self, buf: &mut [u8]) -> Result<usize, Error> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut state = self.state.lock().unwrap();
        loop {
            if let Some(terminal) = &state.terminal {
                return Err(terminal.to_error());
            }
            if let Some(front) = state.chunks.front() {
                let take = front.len().min(buf.len());
                buf[..take].copy_from_slice(&front[..take]);
                if take == front.len() {
                    state.chunks.pop_front();
                } else {
                    let remainder = front.slice(take..);
                    state.chunks[0] = remainder;
                }
                state.bytes -= take;
                state.read_bytes = state.read_bytes.saturating_add(take as u64);
                drop(state);
                self.cond.notify_all();
                self.wake();
                return Ok(take);
            }
            if state.eof {
                return Ok(0);
            }
            if state.closed {
                return Err(Error::Closed);
            }
            state = self.cond.wait(state).unwrap();
        }
    }

    pub(crate) fn next_chunk(&self) -> Result<Option<BodyChunk>, Error> {
        let mut state = self.state.lock().unwrap();
        loop {
            if let Some(terminal) = &state.terminal {
                return Err(terminal.to_error());
            }
            if let Some(front) = state.chunks.pop_front() {
                state.bytes -= front.len();
                let length = front.len();
                if !self.pinned.try_pin(length, &self.exchange_pinned) {
                    let error = Error::Overloaded;
                    state.terminal = Some(Terminal::Overloaded);
                    drop(state);
                    self.cond.notify_all();
                    self.wake();
                    return Err(error);
                }
                drop(state);
                self.cond.notify_all();
                self.wake();
                return Ok(Some(BodyChunk {
                    bytes: front,
                    pinned: self.pinned.clone(),
                    exchange_pinned: self.exchange_pinned.clone(),
                    length,
                }));
            }
            if state.eof {
                return Ok(None);
            }
            if state.closed {
                return Err(Error::Closed);
            }
            state = self.cond.wait(state).unwrap();
        }
    }

    pub(crate) fn write(&self, data: &[u8]) -> Result<usize, Error> {
        if data.is_empty() {
            return Ok(0);
        }
        let mut state = self.state.lock().unwrap();
        loop {
            if let Some(terminal) = &state.terminal {
                return Err(terminal.to_error());
            }
            if state.closed {
                return Err(Error::Closed);
            }
            if state.finished {
                return Err(Error::Closed);
            }
            let available = self.limit.saturating_sub(state.bytes);
            if available == 0 {
                state = self.cond.wait(state).unwrap();
                continue;
            }
            let take = available.min(data.len());
            state.bytes += take;
            state.written_bytes = state.written_bytes.saturating_add(take as u64);
            state
                .chunks
                .push_back(Bytes::copy_from_slice(&data[..take]));
            drop(state);
            self.cond.notify_all();
            self.wake();
            return Ok(take);
        }
    }

    pub(crate) fn write_static(&self, data: &'static [u8]) -> Result<usize, Error> {
        if data.is_empty() {
            return Ok(0);
        }
        let mut state = self.state.lock().unwrap();
        loop {
            if let Some(terminal) = &state.terminal {
                return Err(terminal.to_error());
            }
            if state.closed || state.finished {
                return Err(Error::Closed);
            }
            let available = self.limit.saturating_sub(state.bytes);
            if available == 0 {
                state = self.cond.wait(state).unwrap();
                continue;
            }
            let take = available.min(data.len());
            state.bytes += take;
            state.written_bytes = state.written_bytes.saturating_add(take as u64);
            state.chunks.push_back(Bytes::from_static(&data[..take]));
            drop(state);
            self.cond.notify_all();
            self.wake();
            return Ok(take);
        }
    }

    pub(crate) fn flush(&self) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap();
        loop {
            if let Some(terminal) = &state.terminal {
                return Err(terminal.to_error());
            }
            if state.chunks.is_empty() {
                return Ok(());
            }
            if state.closed {
                return Err(Error::Closed);
            }
            state = self.cond.wait(state).unwrap();
        }
    }

    pub(crate) fn finish(&self) {
        let mut state = self.state.lock().unwrap();
        state.finished = true;
        drop(state);
        self.cond.notify_all();
        self.wake();
    }

    pub(crate) fn mark_eof(&self) {
        let mut state = self.state.lock().unwrap();
        state.eof = true;
        drop(state);
        self.cond.notify_all();
        self.wake();
    }

    pub(crate) fn mark_terminal(&self, terminal: Terminal) {
        let mut state = self.state.lock().unwrap();
        if state.terminal.is_none() {
            state.terminal = Some(terminal);
        }
        drop(state);
        self.cond.notify_all();
        self.wake();
    }

    pub(crate) fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        drop(state);
        self.cond.notify_all();
        self.wake();
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.state.lock().unwrap().finished
    }

    pub(crate) fn is_eof(&self) -> bool {
        self.state.lock().unwrap().eof
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }

    pub(crate) fn available(&self) -> usize {
        self.state.lock().unwrap().bytes
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit
    }

    pub(crate) fn queued(&self) -> usize {
        self.state.lock().unwrap().chunks.len()
    }

    pub(crate) fn read_bytes(&self) -> u64 {
        self.state.lock().unwrap().read_bytes
    }

    pub(crate) fn written_bytes(&self) -> u64 {
        self.state.lock().unwrap().written_bytes
    }

    fn wake(&self) {
        let waker = self.waker.lock().unwrap().clone();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl fmt::Debug for BodyChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock().unwrap();
        f.debug_struct("BodyChannel")
            .field("bytes", &state.bytes)
            .field("chunks", &state.chunks.len())
            .field("eof", &state.eof)
            .field("finished", &state.finished)
            .field("terminal", &state.terminal.is_some())
            .finish()
    }
}

/// A pinned-chunk budget shared by a server or client.
pub(crate) struct PinnedBudget {
    used: AtomicUsize,
    limit: usize,
    exchange_limit: usize,
}

impl PinnedBudget {
    pub(crate) fn new(limit: usize, exchange_limit: usize) -> Self {
        Self {
            used: AtomicUsize::new(0),
            limit,
            exchange_limit,
        }
    }

    pub(crate) fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    fn try_pin(&self, bytes: usize, exchange: &AtomicUsize) -> bool {
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
            if current.saturating_add(bytes) > self.limit {
                return false;
            }
            match self.used.compare_exchange_weak(
                current,
                current + bytes,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        let mut current = exchange.load(Ordering::Relaxed);
        loop {
            if current.saturating_add(bytes) > self.exchange_limit {
                self.used.fetch_sub(bytes, Ordering::AcqRel);
                return false;
            }
            match exchange.compare_exchange_weak(
                current,
                current + bytes,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn unpin(&self, bytes: usize, exchange: &AtomicUsize) {
        self.used.fetch_sub(bytes, Ordering::AcqRel);
        exchange.fetch_sub(bytes, Ordering::AcqRel);
    }
}

/// A refcounted, immutable chunk of a request or response body.
///
/// Retaining a chunk charges the exchange and server pinned-budget; dropping it returns the
/// charge. Chunks are safe to send across threads and to hold after the exchange closes.
pub struct BodyChunk {
    bytes: Bytes,
    pinned: Arc<PinnedBudget>,
    exchange_pinned: Arc<AtomicUsize>,
    length: usize,
}

impl BodyChunk {
    pub(crate) fn from_bytes(
        bytes: Bytes,
        pinned: Arc<PinnedBudget>,
        exchange_pinned: Arc<AtomicUsize>,
    ) -> Self {
        let length = bytes.len();
        Self {
            bytes,
            pinned,
            exchange_pinned,
            length,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl Clone for BodyChunk {
    fn clone(&self) -> Self {
        if self.pinned.try_pin(self.length, &self.exchange_pinned) {
            Self {
                bytes: self.bytes.clone(),
                pinned: self.pinned.clone(),
                exchange_pinned: self.exchange_pinned.clone(),
                length: self.length,
            }
        } else {
            // A clone that cannot be charged yields an empty chunk rather than exceeding the
            // budget. The bytes remain available from the original.
            Self {
                bytes: Bytes::new(),
                pinned: self.pinned.clone(),
                exchange_pinned: self.exchange_pinned.clone(),
                length: 0,
            }
        }
    }
}

impl Drop for BodyChunk {
    fn drop(&mut self) {
        if self.length > 0 {
            self.pinned.unpin(self.length, &self.exchange_pinned);
        }
    }
}

impl fmt::Debug for BodyChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BodyChunk")
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// A source for a blocking body reader.
pub(crate) trait BodySource: Send + Sync {
    fn read_source(&self, buf: &mut [u8]) -> Result<usize, Error>;
    fn next_chunk_source(&self) -> Result<Option<BodyChunk>, Error>;
    fn is_end_source(&self) -> bool;
}

impl BodySource for BodyChannel {
    fn read_source(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.read(buf)
    }

    fn next_chunk_source(&self) -> Result<Option<BodyChunk>, Error> {
        self.next_chunk()
    }

    fn is_end_source(&self) -> bool {
        self.is_eof() && self.available() == 0
    }
}

/// A blocking reader for a request or response body.
///
/// `read` returns `0` at end of body. Reads block until bytes arrive, the body ends, a deadline
/// expires, or the exchange is canceled.
pub struct BodyReader {
    source: Arc<dyn BodySource>,
}

impl BodyReader {
    pub(crate) fn new(channel: Arc<BodyChannel>) -> Self {
        Self { source: channel }
    }

    pub(crate) fn from_source(source: Arc<dyn BodySource>) -> Self {
        Self { source }
    }

    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        self.source.read_source(buf)
    }

    pub fn next_chunk(&mut self) -> Result<Option<BodyChunk>, Error> {
        self.source.next_chunk_source()
    }

    pub fn read_to_end(&mut self, limit: usize) -> Result<Vec<u8>, Error> {
        let mut output = Vec::new();
        let mut buffer = [0_u8; 8192];
        loop {
            let read = self.read(&mut buffer)?;
            if read == 0 {
                return Ok(output);
            }
            if output.len().saturating_add(read) > limit {
                return Err(Error::BodyTooLarge {
                    limit: limit as u64,
                });
            }
            output.extend_from_slice(&buffer[..read]);
        }
    }

    pub fn is_end(&self) -> bool {
        self.source.is_end_source()
    }
}

impl fmt::Debug for BodyReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BodyReader").finish_non_exhaustive()
    }
}

/// An internal sink that frames and enqueues application body bytes.
pub(crate) trait BodySink: Send + Sync {
    fn write(&self, data: &[u8]) -> Result<usize, Error>;
    fn write_static(&self, data: &'static [u8]) -> Result<usize, Error> {
        self.write(data)
    }
    fn flush(&self) -> Result<(), Error>;
    fn finish(&self, trailers: Option<HeaderMap>) -> Result<(), Error>;
}

/// A blocking writer for a request or response body.
///
/// Writes block while the bounded output queue is full; they never perform socket I/O.
pub struct BodyWriter {
    sink: Arc<dyn BodySink>,
}

impl BodyWriter {
    pub(crate) fn new(sink: Arc<dyn BodySink>) -> Self {
        Self { sink }
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        self.sink.write(buf)
    }

    pub fn write_all(&mut self, mut buf: &[u8]) -> Result<(), Error> {
        while !buf.is_empty() {
            let written = self.sink.write(buf)?;
            if written == 0 {
                return Err(Error::Io(std::io::Error::from(
                    std::io::ErrorKind::WriteZero,
                )));
            }
            buf = &buf[written..];
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        self.sink.flush()
    }

    pub fn finish(&mut self) -> Result<(), Error> {
        self.sink.finish(None)
    }

    pub fn finish_with_trailers(&mut self, trailers: HeaderMap) -> Result<(), Error> {
        self.sink.finish(Some(trailers))
    }
}

impl fmt::Debug for BodyWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BodyWriter").finish_non_exhaustive()
    }
}

#[derive(Clone)]
enum BodyBytes {
    Empty,
    Static(&'static [u8]),
    Owned(Bytes),
}

impl BodyBytes {
    fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Static(bytes) => bytes.len(),
            Self::Owned(bytes) => bytes.len(),
        }
    }

    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Empty => b"",
            Self::Static(bytes) => bytes,
            Self::Owned(bytes) => bytes,
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A buffered response body supplied to [`crate::server::Response::set_body`].
#[derive(Clone)]
pub struct ResponseBody {
    inner: BodyBytes,
}

impl ResponseBody {
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    pub(crate) fn into_bytes(self) -> Bytes {
        match self.inner {
            BodyBytes::Empty => Bytes::new(),
            BodyBytes::Static(bytes) => Bytes::from_static(bytes),
            BodyBytes::Owned(bytes) => bytes,
        }
    }
}

impl Default for ResponseBody {
    fn default() -> Self {
        Self {
            inner: BodyBytes::Empty,
        }
    }
}

impl From<()> for ResponseBody {
    fn from(_: ()) -> Self {
        Self::default()
    }
}

impl From<&'static [u8]> for ResponseBody {
    fn from(value: &'static [u8]) -> Self {
        Self {
            inner: BodyBytes::Static(value),
        }
    }
}

impl From<&'static str> for ResponseBody {
    fn from(value: &'static str) -> Self {
        Self::from(value.as_bytes())
    }
}

impl From<String> for ResponseBody {
    fn from(value: String) -> Self {
        Self {
            inner: BodyBytes::Owned(Bytes::from(value.into_bytes())),
        }
    }
}

impl From<Vec<u8>> for ResponseBody {
    fn from(value: Vec<u8>) -> Self {
        Self {
            inner: BodyBytes::Owned(Bytes::from(value)),
        }
    }
}

/// A buffered request body supplied to a client request.
#[derive(Clone)]
pub struct RequestBody {
    inner: BodyBytes,
}

impl RequestBody {
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    pub(crate) fn into_bytes(self) -> Bytes {
        match self.inner {
            BodyBytes::Empty => Bytes::new(),
            BodyBytes::Static(bytes) => Bytes::from_static(bytes),
            BodyBytes::Owned(bytes) => bytes,
        }
    }
}

impl Default for RequestBody {
    fn default() -> Self {
        Self {
            inner: BodyBytes::Empty,
        }
    }
}

impl From<()> for RequestBody {
    fn from(_: ()) -> Self {
        Self::default()
    }
}

impl From<&'static [u8]> for RequestBody {
    fn from(value: &'static [u8]) -> Self {
        Self {
            inner: BodyBytes::Static(value),
        }
    }
}

impl From<&'static str> for RequestBody {
    fn from(value: &'static str) -> Self {
        Self::from(value.as_bytes())
    }
}

impl From<String> for RequestBody {
    fn from(value: String) -> Self {
        Self {
            inner: BodyBytes::Owned(Bytes::from(value.into_bytes())),
        }
    }
}

impl From<Vec<u8>> for RequestBody {
    fn from(value: Vec<u8>) -> Self {
        Self {
            inner: BodyBytes::Owned(Bytes::from(value)),
        }
    }
}

/// A value accepted by `Response::set_body`.
pub trait IntoResponseBody {
    fn into_response_body(self) -> ResponseBody;
}

impl<T: Into<ResponseBody>> IntoResponseBody for T {
    fn into_response_body(self) -> ResponseBody {
        self.into()
    }
}

/// A value accepted by `ClientRequest::body`.
pub trait IntoRequestBody {
    fn into_request_body(self) -> RequestBody;
}

impl<T: Into<RequestBody>> IntoRequestBody for T {
    fn into_request_body(self) -> RequestBody {
        self.into()
    }
}

pub(crate) struct NoopWaker;

impl ChannelWaker for NoopWaker {
    fn wake(&self) {}
}

impl BodyChannel {
    pub(crate) fn closed() -> Arc<Self> {
        let channel = Self::new(
            1,
            Arc::new(PinnedBudget::new(1, 1)),
            Arc::new(AtomicUsize::new(0)),
            Some(Arc::new(NoopWaker)),
        );
        channel.mark_terminal(Terminal::Closed);
        channel
    }
}

impl BodyReader {
    pub(crate) fn closed() -> Self {
        Self::new(BodyChannel::closed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    struct NoopWaker;
    impl ChannelWaker for NoopWaker {
        fn wake(&self) {}
    }

    fn channel(limit: usize) -> Arc<BodyChannel> {
        let pinned = Arc::new(PinnedBudget::new(1024 * 1024, 1024 * 1024));
        BodyChannel::inbound(
            limit,
            pinned,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(NoopWaker),
        )
    }

    #[test]
    fn read_write_and_eof() {
        let channel = channel(16);
        assert!(channel.push(Bytes::from_static(b"hello")));
        assert!(!channel.push(Bytes::from_static(b"this is too long")));
        channel.mark_eof();

        let mut reader = BodyReader::new(channel.clone());
        let mut buffer = [0_u8; 8];
        assert_eq!(reader.read(&mut buffer).unwrap(), 5);
        assert_eq!(&buffer[..5], b"hello");
        assert_eq!(reader.read(&mut buffer).unwrap(), 0);
        assert!(reader.is_end());
    }

    #[test]
    fn next_chunk_is_pinned_and_returns_to_budget() {
        let channel = channel(16);
        let pinned = channel.pinned.clone();
        assert!(channel.push(Bytes::from_static(b"hello")));
        let mut reader = BodyReader::new(channel);
        let chunk = reader.next_chunk().unwrap().unwrap();
        assert_eq!(chunk.as_bytes(), b"hello");
        assert_eq!(pinned.used(), 5);
        drop(chunk);
        assert_eq!(pinned.used(), 0);
    }

    #[test]
    fn blocked_reader_wakes_on_terminal() {
        let channel = channel(16);
        let reader_channel = channel.clone();
        let thread = std::thread::spawn(move || {
            let mut reader = BodyReader::new(reader_channel);
            let mut buffer = [0_u8; 4];
            reader.read(&mut buffer)
        });
        std::thread::sleep(Duration::from_millis(20));
        channel.mark_terminal(Terminal::Canceled);
        assert!(matches!(thread.join().unwrap(), Err(Error::Canceled)));
    }

    #[test]
    fn write_blocks_until_capacity_then_succeeds() {
        let channel = channel(4);
        let writer_channel = channel.clone();
        let done = Arc::new(AtomicBool::new(false));
        let done_thread = done.clone();
        let thread = std::thread::spawn(move || {
            let mut writer = BodyWriter::new(Arc::new(ChannelSink(writer_channel)));
            writer.write_all(b"abcdef").unwrap();
            done_thread.store(true, Ordering::Relaxed);
        });
        std::thread::sleep(Duration::from_millis(20));
        assert!(!done.load(Ordering::Relaxed));
        assert_eq!(channel.take().unwrap().as_ref(), b"abcd");
        thread.join().unwrap();
        assert!(done.load(Ordering::Relaxed));
    }

    struct ChannelSink(Arc<BodyChannel>);
    impl BodySink for ChannelSink {
        fn write(&self, data: &[u8]) -> Result<usize, Error> {
            self.0.write(data)
        }
        fn flush(&self) -> Result<(), Error> {
            self.0.flush()
        }
        fn finish(&self, _trailers: Option<HeaderMap>) -> Result<(), Error> {
            self.0.finish();
            Ok(())
        }
    }
}
