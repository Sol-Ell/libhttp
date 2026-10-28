use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Instant;

use crate::body::{
    BodyChannel, BodySink, BodyWriter, ChannelWaker, PinnedBudget, ResponseBody, Terminal,
};
use crate::h1::{BodyFraming, ResponseBodyEncoding, encode_last_chunk, encode_response_head};
use crate::header::HeaderMap;
use crate::{Error, Method, ProtocolErrorKind, StatusCode, TimeoutKind, Version};

use super::slab::ConnId;
use super::worker::ReactorWaker;
use crate::server::ServerShared;

/// A per-connection exchange between the reactor and one worker or deferred application thread.
pub(crate) struct Exchange {
    pub(crate) seq: u64,
    pub(crate) conn: ConnId,
    pub(crate) shared: Arc<ServerShared>,
    pub(crate) method: Method,
    pub(crate) version: Version,
    pub(crate) keep_alive: bool,
    pub(crate) framing: BodyFraming,
    pub(crate) expect_continue: bool,
    pub(crate) in_body: Arc<BodyChannel>,
    pub(crate) out_body: Arc<BodyChannel>,
    pub(crate) meta: Mutex<ExchangeMeta>,
    pub(crate) cond: Condvar,
    pub(crate) started: Instant,
    pub(crate) waker: Arc<ExchangeWaker>,
    pub(crate) peer_addr: Option<SocketAddr>,
    pub(crate) local_addr: Option<SocketAddr>,
    pub(crate) secure: bool,
    pub(crate) trailers: Arc<OnceLock<HeaderMap>>,
}

/// Mutable response and lifecycle state shared by the handler, deferred handles and the reactor.
pub(crate) struct ExchangeMeta {
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Option<ResponseBody>,
    pub(crate) body_writer_started: bool,
    pub(crate) committed: bool,
    pub(crate) finished: bool,
    pub(crate) framing: Option<ResponseBodyEncoding>,
    pub(crate) close: bool,
    pub(crate) terminal: Option<Terminal>,
    pub(crate) request_body_eof: bool,
    pub(crate) deferred: bool,
    pub(crate) handler_done: bool,
    pub(crate) request_aborted: bool,
    pub(crate) continue_sent: bool,
    pub(crate) invalid: Option<&'static str>,
}

pub(crate) struct ExchangeWaker {
    reactor: Arc<ReactorWaker>,
    conn: ConnId,
}

impl ChannelWaker for ExchangeWaker {
    fn wake(&self) {
        self.reactor.wake_conn(self.conn);
    }
}

impl Exchange {
    pub(crate) fn new(
        seq: u64,
        conn: ConnId,
        shared: Arc<ServerShared>,
        method: Method,
        version: Version,
        keep_alive: bool,
        framing: BodyFraming,
        expect_continue: bool,
        waker: Arc<ReactorWaker>,
        peer_addr: Option<SocketAddr>,
        local_addr: Option<SocketAddr>,
    ) -> Arc<Self> {
        let limits = &shared.limits;
        let exchange_pinned = Arc::new(AtomicUsize::new(0));
        let exchange_waker = Arc::new(ExchangeWaker {
            reactor: waker,
            conn,
        });
        let in_body = BodyChannel::inbound(
            limits.max_request_body_buffer,
            shared.pinned.clone(),
            exchange_pinned.clone(),
            exchange_waker.clone(),
        );
        let out_body = BodyChannel::outbound(
            limits.max_response_body_buffer,
            shared.pinned.clone(),
            exchange_pinned,
            exchange_waker.clone(),
        );
        Arc::new(Self {
            seq,
            conn,
            shared,
            method,
            version,
            keep_alive,
            framing,
            expect_continue,
            in_body,
            out_body,
            meta: Mutex::new(ExchangeMeta {
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                body: None,
                body_writer_started: false,
                committed: false,
                finished: false,
                framing: None,
                close: false,
                terminal: None,
                request_body_eof: framing == BodyFraming::None,
                deferred: false,
                handler_done: false,
                request_aborted: false,
                continue_sent: false,
                invalid: None,
            }),
            cond: Condvar::new(),
            started: Instant::now(),
            waker: exchange_waker,
            peer_addr,
            local_addr,
            secure: false,
            trailers: Arc::new(OnceLock::new()),
        })
    }

    pub(crate) fn set_trailers(&self, trailers: HeaderMap) {
        let _ = self.trailers.set(trailers);
    }

    pub(crate) fn wake_reactor(&self) {
        self.waker.wake();
    }

    pub(crate) fn mark_request_body_eof(&self) {
        {
            let mut meta = self.meta.lock().unwrap();
            meta.request_body_eof = true;
        }
        self.in_body.mark_eof();
        self.cond.notify_all();
        self.waker.wake();
    }

    pub(crate) fn request_body_eof(&self) -> bool {
        self.meta.lock().unwrap().request_body_eof
    }

    pub(crate) fn request_body_consumed(&self) -> bool {
        let eof = self.meta.lock().unwrap().request_body_eof;
        eof && self.in_body.available() == 0
    }

    pub(crate) fn terminal(&self, terminal: Terminal) {
        let notify = {
            let mut meta = self.meta.lock().unwrap();
            if meta.terminal.is_none() {
                meta.terminal = Some(terminal.clone());
                meta.close = true;
                true
            } else {
                false
            }
        };
        self.in_body.mark_terminal(terminal.clone());
        self.out_body.mark_terminal(terminal);
        if notify {
            self.cond.notify_all();
            self.waker.wake();
        }
    }

    pub(crate) fn terminal_error(&self) -> Option<Error> {
        self.meta
            .lock()
            .unwrap()
            .terminal
            .as_ref()
            .map(Terminal::to_error)
    }

    pub(crate) fn is_terminal(&self) -> bool {
        self.meta.lock().unwrap().terminal.is_some()
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.meta.lock().unwrap().finished
    }

    pub(crate) fn is_committed(&self) -> bool {
        self.meta.lock().unwrap().committed
    }

    pub(crate) fn response_done(&self) -> bool {
        let meta = self.meta.lock().unwrap();
        meta.finished && self.out_body.available() == 0
    }

    pub(crate) fn close_after_response(&self) -> bool {
        let mut meta = self.meta.lock().unwrap();
        if !self.keep_alive || !meta.request_body_eof || self.in_body.available() > 0 {
            meta.close = true;
        }
        meta.close
    }

    pub(crate) fn mark_continue_sent(&self) {
        let mut meta = self.meta.lock().unwrap();
        if !meta.continue_sent && !meta.committed {
            meta.continue_sent = true;
            drop(meta);
            let _ = self.out_body.write(b"HTTP/1.1 100 Continue\r\n\r\n");
        }
    }

    pub(crate) fn abort_request(&self) {
        self.meta.lock().unwrap().request_aborted = true;
        self.in_body.mark_terminal(Terminal::Canceled);
        self.waker.wake();
    }

    pub(crate) fn commit_head_locked(
        &self,
        meta: &mut ExchangeMeta,
        body: ResponseBodyEncoding,
    ) -> Result<(), Error> {
        if meta.committed {
            return Ok(());
        }
        if meta.status.is_informational() {
            return Err(Error::InvalidRequest(
                "1xx status is library-managed and cannot be a final response",
            ));
        }
        if meta.headers.contains("content-length") || meta.headers.contains("transfer-encoding") {
            return Err(Error::InvalidRequest(
                "content-length and transfer-encoding are library-managed",
            ));
        }
        let no_body = self.method == Method::Head || meta.status.is_empty_body();
        let body = if no_body {
            ResponseBodyEncoding::None
        } else {
            body
        };
        let body = match (body, self.version) {
            (ResponseBodyEncoding::Chunked, Version::Http10) => ResponseBodyEncoding::UntilClose,
            (body, _) => body,
        };
        let close = !self.keep_alive || !meta.request_body_eof || self.in_body.available() > 0;
        let mut head = Vec::with_capacity(256);
        encode_response_head(
            &mut head,
            meta.status,
            self.version,
            &meta.headers,
            body,
            close,
        )?;
        write_channel_all(&self.out_body, &head)?;
        meta.committed = true;
        meta.framing = Some(body);
        meta.close = close;
        Ok(())
    }

    pub(crate) fn commit_fixed_locked(
        &self,
        meta: &mut ExchangeMeta,
        body: ResponseBody,
    ) -> Result<(), Error> {
        if meta.committed {
            return Ok(());
        }
        let bytes = body.into_bytes();
        self.commit_head_locked(
            meta,
            ResponseBodyEncoding::ContentLength(bytes.len() as u64),
        )?;
        if !matches!(meta.framing, Some(ResponseBodyEncoding::None)) {
            write_channel_all(&self.out_body, &bytes)?;
        }
        meta.finished = true;
        self.out_body.finish();
        Ok(())
    }

    pub(crate) fn commit_stream_locked(&self, meta: &mut ExchangeMeta) -> Result<(), Error> {
        if meta.committed {
            return Ok(());
        }
        self.commit_head_locked(meta, ResponseBodyEncoding::Chunked)?;
        Ok(())
    }

    pub(crate) fn finish_stream_locked(
        &self,
        meta: &mut ExchangeMeta,
        trailers: Option<HeaderMap>,
    ) -> Result<(), Error> {
        if meta.finished {
            return Ok(());
        }
        if !meta.committed {
            self.commit_stream_locked(meta)?;
        }
        match meta.framing {
            Some(ResponseBodyEncoding::Chunked) => {
                let mut tail = Vec::new();
                encode_last_chunk(&mut tail, trailers.as_ref())?;
                write_channel_all(&self.out_body, &tail)?;
            }
            Some(ResponseBodyEncoding::UntilClose) | Some(ResponseBodyEncoding::None) => {
                if trailers.is_some() {
                    return Err(Error::InvalidRequest("trailers require chunked framing"));
                }
            }
            Some(ResponseBodyEncoding::ContentLength(_)) | None => {
                return Err(Error::InvalidRequest("response is not streamed"));
            }
        }
        meta.finished = true;
        self.out_body.finish();
        self.waker.wake();
        Ok(())
    }

    pub(crate) fn reset_for_error(&self) -> Result<(), Error> {
        let mut meta = self.meta.lock().unwrap();
        if meta.committed {
            meta.close = true;
            return Ok(());
        }
        meta.status = StatusCode::INTERNAL_SERVER_ERROR;
        meta.headers.clear();
        meta.body = None;
        meta.body_writer_started = false;
        meta.invalid = None;
        self.commit_fixed_locked(&mut meta, ResponseBody::default())
    }

    pub(crate) fn ensure_head_for_empty(&self) -> Result<(), Error> {
        let mut meta = self.meta.lock().unwrap();
        if meta.committed {
            return Ok(());
        }
        self.commit_fixed_locked(&mut meta, ResponseBody::default())
    }

    pub(crate) fn fail_overloaded(&self, status: StatusCode) {
        let mut meta = self.meta.lock().unwrap();
        if meta.committed {
            meta.close = true;
            self.out_body.finish();
            return;
        }
        meta.status = status;
        meta.headers.clear();
        let _ = self.commit_fixed_locked(&mut meta, ResponseBody::default());
        meta.close = true;
    }
}

/// The `BodySink` used by a server response writer.
pub(crate) struct ResponseSink {
    exchange: Arc<Exchange>,
}

impl ResponseSink {
    pub(crate) fn new(exchange: Arc<Exchange>) -> Self {
        Self { exchange }
    }

    fn write_once(&self, data: &[u8]) -> Result<usize, Error> {
        let mut meta = self.exchange.meta.lock().unwrap();
        if let Some(terminal) = &meta.terminal {
            return Err(terminal.to_error());
        }
        if meta.finished {
            return Err(Error::Closed);
        }
        if let Some(reason) = meta.invalid {
            return Err(Error::InvalidRequest(reason));
        }
        if !meta.committed {
            self.exchange.commit_stream_locked(&mut meta)?;
        }
        if meta.framing == Some(ResponseBodyEncoding::None) {
            return Ok(data.len());
        }
        let take = data.len().min(16 * 1024);
        match meta.framing {
            Some(ResponseBodyEncoding::Chunked) => {
                let mut framed = Vec::with_capacity(take + 16);
                crate::h1::encode_chunk(&mut framed, &data[..take]);
                write_channel_all(&self.exchange.out_body, &framed)?;
            }
            Some(ResponseBodyEncoding::UntilClose) => {
                write_channel_all(&self.exchange.out_body, &data[..take])?;
            }
            _ => return Err(Error::InvalidRequest("invalid response framing")),
        }
        Ok(take)
    }
}

impl BodySink for ResponseSink {
    fn write(&self, data: &[u8]) -> Result<usize, Error> {
        self.write_once(data)
    }

    fn flush(&self) -> Result<(), Error> {
        self.exchange.out_body.flush()
    }

    fn finish(&self, trailers: Option<HeaderMap>) -> Result<(), Error> {
        let mut meta = self.exchange.meta.lock().unwrap();
        if meta.finished {
            return Ok(());
        }
        if let Some(terminal) = &meta.terminal {
            return Err(terminal.to_error());
        }
        self.exchange.finish_stream_locked(&mut meta, trailers)
    }
}

fn write_channel_all(channel: &BodyChannel, data: &[u8]) -> Result<(), Error> {
    let mut offset = 0;
    while offset < data.len() {
        let written = channel.write(&data[offset..])?;
        if written == 0 {
            return Err(Error::Io(std::io::Error::from(
                std::io::ErrorKind::WriteZero,
            )));
        }
        offset += written;
    }
    Ok(())
}

/// The public-visible response writer's internal constructor.
pub(crate) fn response_writer(exchange: Arc<Exchange>) -> BodyWriter {
    BodyWriter::new(Arc::new(ResponseSink::new(exchange)))
}

impl Exchange {
    pub(crate) fn protocol_error(&self, kind: ProtocolErrorKind, detail: &'static str) {
        self.terminal(Terminal::Protocol(crate::ProtocolError::new(
            self.version,
            kind,
            detail,
        )));
    }

    pub(crate) fn timeout(&self, kind: TimeoutKind) {
        self.terminal(Terminal::Timeout(kind));
    }

    pub(crate) fn over_pinned(&self) {
        self.terminal(Terminal::Overloaded);
    }

    pub(crate) fn deferred_count(&self) -> usize {
        self.shared.deferred.load(Ordering::Relaxed)
    }

    pub(crate) fn ensure_deferred_capacity(&self) -> bool {
        let max = self.shared.limits.max_deferred_exchanges;
        let mut current = self.shared.deferred.load(Ordering::Relaxed);
        loop {
            if current >= max {
                return false;
            }
            match self.shared.deferred.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    pub(crate) fn release_deferred(&self) {
        self.shared.deferred.fetch_sub(1, Ordering::AcqRel);
    }

    pub(crate) fn pinned(&self) -> &PinnedBudget {
        &self.shared.pinned
    }
}
