use std::sync::Arc;

use crate::body::{BodySink, BodyWriter, IntoResponseBody, Terminal};
use crate::header::{
    AsHeaderName, Header, HeaderError, HeaderMap, IntoHeaderName, IntoHeaderValue,
};
use crate::runtime::Exchange;
use crate::{Error, StatusCode};

use super::deferred::DeferredParts;

/// A response being built by a handler.
pub struct Response {
    exchange: Option<Arc<Exchange>>,
    status: StatusCode,
    headers: HeaderMap,
    body: Option<BodyWriter>,
    body_set: bool,
    body_writer_started: bool,
    tombstone: bool,
    invalid: Option<&'static str>,
}

impl Response {
    /// Creates a detached response with status `200 OK`.
    pub fn new() -> Self {
        Self {
            exchange: None,
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: None,
            body_set: false,
            body_writer_started: false,
            tombstone: false,
            invalid: None,
        }
    }

    pub(crate) fn attached(exchange: Arc<Exchange>) -> Self {
        Self {
            exchange: Some(exchange),
            ..Self::new()
        }
    }

    pub(crate) fn from_parts(exchange: Arc<Exchange>, parts: DeferredParts) -> Self {
        let body_writer_started = parts.body.is_some();
        Self {
            exchange: Some(exchange),
            status: parts.status,
            headers: parts.headers,
            body: parts.body,
            body_set: parts.body_set,
            body_writer_started,
            tombstone: false,
            invalid: None,
        }
    }

    pub(crate) fn tombstone(exchange: Arc<Exchange>) -> Self {
        Self {
            exchange: Some(exchange),
            status: StatusCode::INTERNAL_SERVER_ERROR,
            headers: HeaderMap::new(),
            body: None,
            body_set: false,
            body_writer_started: false,
            tombstone: true,
            invalid: None,
        }
    }

    /// Returns the current status.
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Sets the status.
    pub fn set_status(&mut self, status: StatusCode) -> &mut Self {
        if self.tombstone {
            return self;
        }
        self.status = status;
        if status.is_informational() {
            self.invalid = Some("1xx status is library-managed");
        }
        if let Some(exchange) = &self.exchange {
            let mut meta = exchange.meta.lock().unwrap();
            meta.status = status;
            if status.is_informational() {
                meta.invalid = Some("1xx status is library-managed");
            }
        }
        self
    }

    /// Returns the response headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Replaces a typed header.
    pub fn set<T: Header>(&mut self, value: T) -> &mut Self {
        if self.tombstone {
            return self;
        }
        self.headers.set(value);
        self.sync_headers();
        self
    }

    /// Appends a typed header, rejecting a duplicate singleton.
    pub fn append<T: Header>(&mut self, value: T) -> Result<&mut Self, HeaderError> {
        if self.tombstone {
            return Ok(self);
        }
        self.headers.append(value)?;
        self.sync_headers();
        Ok(self)
    }

    /// Removes a typed header.
    pub fn remove<T: Header>(&mut self) -> &mut Self {
        if self.tombstone {
            return self;
        }
        self.headers.remove::<T>();
        self.sync_headers();
        self
    }

    /// Adds a raw header after validation.
    pub fn header(
        &mut self,
        name: impl IntoHeaderName,
        value: impl IntoHeaderValue,
    ) -> Result<&mut Self, Error> {
        if self.tombstone {
            return Err(Error::Closed);
        }
        self.headers.append_raw(name, value)?;
        self.sync_headers();
        Ok(self)
    }

    /// Removes every field with this name.
    pub fn remove_header(&mut self, name: impl AsHeaderName) -> &mut Self {
        if self.tombstone {
            return self;
        }
        self.headers.remove_header(name);
        self.sync_headers();
        self
    }

    /// Returns the streaming response body writer.
    pub fn body(&mut self) -> &mut BodyWriter {
        if self.body.is_none() {
            let sink: Arc<dyn BodySink> = if self.tombstone {
                Arc::new(ErrorSink(Terminal::Closed))
            } else if self.body_set {
                Arc::new(ErrorSink(Terminal::InvalidRequest(
                    "body was already set with set_body",
                )))
            } else if let Some(exchange) = &self.exchange {
                self.body_writer_started = true;
                exchange.meta.lock().unwrap().body_writer_started = true;
                Arc::new(crate::runtime::ResponseSink::new(exchange.clone()))
            } else {
                Arc::new(ErrorSink(Terminal::Closed))
            };
            self.body = Some(BodyWriter::new(sink));
        }
        self.body.as_mut().expect("body writer was initialized")
    }

    /// Sets a buffered response body.
    pub fn set_body(&mut self, body: impl IntoResponseBody) -> &mut Self {
        if self.tombstone {
            return self;
        }
        let body = body.into_response_body();
        if self.body_writer_started {
            self.invalid = Some("body was already streamed");
            if let Some(exchange) = &self.exchange {
                exchange.meta.lock().unwrap().invalid = self.invalid;
            }
            return self;
        }
        self.body_set = true;
        if let Some(exchange) = &self.exchange {
            exchange.meta.lock().unwrap().body = Some(body.clone());
        }
        self
    }

    /// Whether the exchange was canceled.
    pub fn is_canceled(&self) -> bool {
        self.exchange
            .as_ref()
            .is_some_and(|exchange| exchange.is_terminal() || self.tombstone)
    }

    pub(crate) fn exchange(&self) -> &Arc<Exchange> {
        self.exchange.as_ref().expect("attached response")
    }

    pub(crate) fn take_body_writer(&mut self) -> Option<BodyWriter> {
        self.body.take()
    }

    pub(crate) fn can_defer(&self) -> bool {
        if self.tombstone || self.body_set || self.body_writer_started || self.exchange.is_none() {
            return false;
        }
        let exchange = self.exchange.as_ref().unwrap();
        !exchange.is_committed() && !exchange.is_terminal()
    }

    pub(crate) fn take_for_deferred(&mut self) -> DeferredParts {
        let exchange = self.exchange.clone().expect("attached response");
        let parts = DeferredParts {
            status: self.status,
            headers: self.headers.clone(),
            body: self.body.take(),
            body_set: self.body_set,
        };
        *self = Self::tombstone(exchange);
        parts
    }

    pub(crate) fn finalize(&mut self, result: Result<(), Error>) {
        let Some(exchange) = &self.exchange else {
            return;
        };
        if self.tombstone {
            return;
        }
        let failed = result.is_err();
        if failed {
            let committed = exchange.is_committed();
            if committed {
                exchange.meta.lock().unwrap().close = true;
            } else {
                let _ = exchange.reset_for_error();
            }
        } else if let Some(writer) = self.body.take() {
            let mut writer = writer;
            if writer.finish().is_err() {
                exchange.meta.lock().unwrap().close = true;
            }
        } else if self.body_set {
            let body = exchange
                .meta
                .lock()
                .unwrap()
                .body
                .clone()
                .unwrap_or_default();
            let failed = {
                let mut meta = exchange.meta.lock().unwrap();
                exchange.commit_fixed_locked(&mut meta, body).is_err()
            };
            if failed {
                let _ = exchange.reset_for_error();
            }
        } else if exchange.ensure_head_for_empty().is_err() {
            let _ = exchange.reset_for_error();
        }
        if let Some(reason) = self.invalid {
            let mut meta = exchange.meta.lock().unwrap();
            if !meta.committed {
                meta.invalid = Some(reason);
            }
        }
        exchange.out_body.finish();
        {
            let mut meta = exchange.meta.lock().unwrap();
            meta.handler_done = true;
            if !meta.request_body_eof || exchange.in_body.available() > 0 {
                meta.close = true;
            }
        }
        if failed {
            exchange
                .shared
                .stats
                .failures
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        exchange.wake_reactor();
    }

    fn sync_headers(&self) {
        if let Some(exchange) = &self.exchange {
            exchange.meta.lock().unwrap().headers = self.headers.clone();
        }
    }
}

impl Default for Response {
    fn default() -> Self {
        Self::new()
    }
}

struct ErrorSink(Terminal);

impl BodySink for ErrorSink {
    fn write(&self, _data: &[u8]) -> Result<usize, Error> {
        Err(self.0.to_error())
    }

    fn flush(&self) -> Result<(), Error> {
        Err(self.0.to_error())
    }

    fn finish(&self, _trailers: Option<HeaderMap>) -> Result<(), Error> {
        Err(self.0.to_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HeaderValue;

    #[test]
    fn detached_response_keeps_headers() {
        let mut response = Response::new();
        response.set_status(StatusCode::CREATED);
        response
            .header("x-test", HeaderValue::from_static("yes"))
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response.headers().get_raw("x-test").unwrap().as_bytes(),
            b"yes"
        );
        assert!(!response.can_defer());
    }

    #[test]
    fn one_hundred_is_rejected_by_status_tracking() {
        let mut response = Response::new();
        response.set_status(StatusCode::CONTINUE);
        assert_eq!(response.status(), StatusCode::CONTINUE);
        assert!(response.invalid.is_some());
    }
}
