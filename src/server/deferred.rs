use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::OnceLock;

use crate::body::{BodyReader, BodyWriter};
use crate::header::{AsHeaderName, Header, HeaderMap, HeaderRef};
use crate::runtime::Exchange;
use crate::{Error, Method, Target, TimeoutKind, Version};

use super::response::Response;

pub(crate) type FallbackHook =
    Box<dyn FnOnce(&mut Response, &DeferredFailure) -> Result<(), Error> + Send + 'static>;

pub(crate) struct DeferredParts {
    pub(crate) status: crate::StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Option<BodyWriter>,
    pub(crate) body_set: bool,
}

/// Why a deferred exchange reached a terminal state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeferredFailureReason {
    Timeout(TimeoutKind),
    Shutdown,
    ApplicationError,
    Dropped,
    Internal,
}

/// Metadata passed to a deferred terminal fallback.
#[derive(Clone, Debug)]
pub struct DeferredFailure {
    pub reason: DeferredFailureReason,
    pub method: Method,
    pub target: String,
    pub version: Version,
    pub elapsed: std::time::Duration,
    pub request_body_consumed: bool,
}

/// The request side of a deferred exchange.
pub struct DeferredRequest {
    pub(crate) head: crate::h1::RequestHead,
    pub(crate) body: BodyReader,
    pub(crate) exchange: Arc<Exchange>,
    pub(crate) trailers: Arc<OnceLock<HeaderMap>>,
    pub(crate) not_sync: PhantomData<Cell<()>>,
}

impl DeferredRequest {
    pub fn method(&self) -> Method {
        self.head.method.clone()
    }

    pub fn target(&self) -> Target<'_> {
        self.head.target().unwrap_or(Target::Asterisk)
    }

    pub fn version(&self) -> Version {
        self.head.version
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.head.headers
    }

    pub fn header<T: Header>(&self) -> Option<&T> {
        self.head.headers.get::<T>()
    }

    pub fn headers_of<T: Header>(&self) -> impl Iterator<Item = &T> {
        self.head.headers.get_all::<T>()
    }

    pub fn header_value(&self, name: impl AsHeaderName) -> Option<HeaderRef<'_>> {
        self.head.headers.get_raw(name)
    }

    pub fn body(&mut self) -> &mut BodyReader {
        &mut self.body
    }

    pub fn trailers(&self) -> Option<&HeaderMap> {
        self.trailers.get()
    }

    pub fn peer_addr(&self) -> Option<std::net::SocketAddr> {
        self.exchange.peer_addr
    }

    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.exchange.local_addr
    }

    pub fn is_secure(&self) -> bool {
        self.exchange.secure
    }

    pub fn is_canceled(&self) -> bool {
        self.exchange.is_terminal()
    }
}

impl Drop for DeferredRequest {
    fn drop(&mut self) {
        if !self.body.is_end() {
            self.exchange.meta.lock().unwrap().close = true;
        }
    }
}

/// The response side of a deferred exchange.
pub struct DeferredResponse {
    pub(crate) response: Response,
    fallback: Option<FallbackHook>,
    method: Method,
    target: String,
    version: Version,
    finished: bool,
    released: bool,
    not_sync: PhantomData<Cell<()>>,
}

impl DeferredResponse {
    pub(crate) fn new(
        exchange: Arc<Exchange>,
        parts: DeferredParts,
        fallback: Option<FallbackHook>,
        method: Method,
        target: String,
        version: Version,
    ) -> Self {
        Self {
            response: Response::from_parts(exchange, parts),
            fallback,
            method,
            target,
            version,
            finished: false,
            released: false,
            not_sync: PhantomData,
        }
    }

    pub fn status(&self) -> crate::StatusCode {
        self.response.status()
    }

    pub fn set_status(&mut self, status: crate::StatusCode) -> &mut Self {
        self.response.set_status(status);
        self
    }

    pub fn headers(&self) -> &HeaderMap {
        self.response.headers()
    }

    pub fn set<T: Header>(&mut self, value: T) -> &mut Self {
        self.response.set(value);
        self
    }

    pub fn append<T: Header>(&mut self, value: T) -> Result<&mut Self, crate::header::HeaderError> {
        self.response.append(value)?;
        Ok(self)
    }

    pub fn remove<T: Header>(&mut self) -> &mut Self {
        self.response.remove::<T>();
        self
    }

    pub fn header(
        &mut self,
        name: impl crate::header::IntoHeaderName,
        value: impl crate::header::IntoHeaderValue,
    ) -> Result<&mut Self, Error> {
        self.response.header(name, value)?;
        Ok(self)
    }

    pub fn body(&mut self) -> &mut BodyWriter {
        self.response.body()
    }

    pub fn set_body(&mut self, body: impl crate::body::IntoResponseBody) -> &mut Self {
        self.response.set_body(body);
        self
    }

    pub fn is_canceled(&self) -> bool {
        self.response.is_canceled()
    }

    pub fn finish(&mut self) -> Result<(), Error> {
        if self.finished {
            return Ok(());
        }
        self.response.finalize(Ok(()));
        self.finished = true;
        self.release();
        Ok(())
    }

    pub fn finish_with_trailers(&mut self, trailers: HeaderMap) -> Result<(), Error> {
        if self.finished {
            return Ok(());
        }
        let mut writer = self
            .response
            .take_body_writer()
            .ok_or(Error::InvalidRequest("no streaming body writer"))?;
        writer.finish_with_trailers(trailers)?;
        drop(writer);
        self.response.finalize(Ok(()));
        self.finished = true;
        self.release();
        Ok(())
    }

    pub fn send(mut self) -> Result<(), Error> {
        self.finish()
    }

    pub fn send_body(self, body: impl crate::body::IntoResponseBody) -> Result<(), Error> {
        let mut this = self;
        this.response.set_body(body);
        this.finish()
    }

    fn release(&mut self) {
        if !self.released {
            self.released = true;
            self.response_exchange().release_deferred();
        }
    }

    fn response_exchange(&self) -> &Arc<Exchange> {
        self.response.exchange()
    }
}

impl Drop for DeferredResponse {
    fn drop(&mut self) {
        if !self.finished && !self.released {
            let exchange = self.response_exchange().clone();
            if self.fallback.is_some() && !exchange.is_committed() {
                let hook = self.fallback.take().expect("checked");
                let mut response = Response::attached(exchange.clone());
                let failure = DeferredFailure {
                    reason: DeferredFailureReason::Dropped,
                    method: self.method.clone(),
                    target: self.target.clone(),
                    version: self.version,
                    elapsed: exchange.started.elapsed(),
                    request_body_consumed: exchange.request_body_consumed(),
                };
                if hook(&mut response, &failure).is_err() {
                    let _ = exchange.reset_for_error();
                } else {
                    response.finalize(Ok(()));
                }
            } else if !exchange.is_committed() {
                let _ = exchange.reset_for_error();
            } else {
                exchange.meta.lock().unwrap().close = true;
            }
        }
        self.release();
    }
}
