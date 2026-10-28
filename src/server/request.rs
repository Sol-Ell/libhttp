use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;

use crate::body::BodyReader;
use crate::h1::RequestHead;
use crate::header::{AsHeaderName, Header, HeaderMap, HeaderRef};
use crate::runtime::Exchange;
use crate::{Error, Method, Target, Version};

use super::deferred::{DeferredFailure, DeferredRequest, DeferredResponse};
use super::response::Response;

/// A request handed to a [`super::Handler`].
pub struct Request {
    exchange: Arc<Exchange>,
    head: Option<RequestHead>,
    body: BodyReader,
    trailers: Arc<OnceLock<HeaderMap>>,
    deferred: bool,
}

impl Request {
    pub(crate) fn new(exchange: Arc<Exchange>, head: RequestHead) -> Self {
        let body = BodyReader::new(exchange.in_body.clone());
        let trailers = exchange.trailers.clone();
        Self {
            exchange,
            head: Some(head),
            body,
            trailers,
            deferred: false,
        }
    }

    /// The request method.
    pub fn method(&self) -> Method {
        self.head
            .as_ref()
            .map(|head| head.method.clone())
            .unwrap_or(Method::Get)
    }

    /// The parsed request target.
    pub fn target(&self) -> Target<'_> {
        self.head
            .as_ref()
            .and_then(|head| head.target().ok())
            .unwrap_or(Target::Asterisk)
    }

    /// The request version.
    pub fn version(&self) -> Version {
        self.head
            .as_ref()
            .map(|head| head.version)
            .unwrap_or(Version::Http11)
    }

    /// The request headers.
    pub fn headers(&self) -> &HeaderMap {
        match &self.head {
            Some(head) => &head.headers,
            None => empty_headers(),
        }
    }

    /// Returns the first typed header of type `T`.
    pub fn header<T: Header>(&self) -> Option<&T> {
        self.head.as_ref()?.headers.get::<T>()
    }

    /// Iterates over all typed headers of type `T`.
    pub fn headers_of<T: Header>(&self) -> impl Iterator<Item = &T> {
        self.head
            .as_ref()
            .into_iter()
            .flat_map(|head| head.headers.get_all::<T>())
    }

    /// Returns the first raw header value with `name`.
    pub fn header_value(&self, name: impl AsHeaderName) -> Option<HeaderRef<'_>> {
        self.head.as_ref()?.headers.get_raw(name)
    }

    /// The streaming request body.
    pub fn body(&mut self) -> &mut BodyReader {
        &mut self.body
    }

    /// Request trailers, available after the body reaches EOF.
    pub fn trailers(&self) -> Option<&HeaderMap> {
        self.trailers.get()
    }

    /// The peer address.
    pub fn peer_addr(&self) -> Option<std::net::SocketAddr> {
        self.exchange.peer_addr
    }

    /// The local address.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.exchange.local_addr
    }

    /// Whether the connection uses TLS.
    pub fn is_secure(&self) -> bool {
        self.exchange.secure
    }

    /// Whether the exchange was canceled.
    pub fn is_canceled(&self) -> bool {
        self.deferred || self.exchange.is_terminal()
    }

    /// Atomically moves this request and the uncommitted response into deferred handles.
    pub fn defer(
        &mut self,
        response: &mut Response,
    ) -> Result<(DeferredRequest, DeferredResponse), Error> {
        self.defer_inner(response, None)
    }

    /// Defers with a terminal fallback hook.
    pub fn defer_with_fallback<F>(
        &mut self,
        response: &mut Response,
        fallback: F,
    ) -> Result<(DeferredRequest, DeferredResponse), Error>
    where
        F: FnOnce(&mut Response, &DeferredFailure) -> Result<(), Error> + Send + 'static,
    {
        self.defer_inner(response, Some(Box::new(fallback)))
    }

    fn defer_inner(
        &mut self,
        response: &mut Response,
        fallback: Option<super::deferred::FallbackHook>,
    ) -> Result<(DeferredRequest, DeferredResponse), Error> {
        if self.deferred || self.head.is_none() {
            return Err(Error::InvalidRequest("request is already deferred"));
        }
        if !response.can_defer() {
            return Err(Error::InvalidRequest(
                "response is committed, streamed, or has a body",
            ));
        }
        if !self.exchange.ensure_deferred_capacity() {
            return Err(Error::Overloaded);
        }
        let head = self.head.take().expect("checked above");
        let body = std::mem::replace(&mut self.body, BodyReader::closed());
        let parts = response.take_for_deferred();
        self.deferred = true;
        self.exchange.meta.lock().unwrap().deferred = true;
        let exchange = self.exchange.clone();
        let trailers = self.trailers.clone();
        let method = head.method.clone();
        let target = String::from_utf8_lossy(&head.target).into_owned();
        let version = head.version;
        let request = DeferredRequest {
            head,
            body,
            exchange: exchange.clone(),
            trailers,
            not_sync: PhantomData::<Cell<()>>,
        };
        let response = DeferredResponse::new(exchange, parts, fallback, method, target, version);
        Ok((request, response))
    }

    pub(crate) fn exchange(&self) -> &Arc<Exchange> {
        &self.exchange
    }

    pub(crate) fn started(&self) -> Instant {
        self.exchange.started
    }
}

fn empty_headers() -> &'static HeaderMap {
    static EMPTY: OnceLock<HeaderMap> = OnceLock::new();
    EMPTY.get_or_init(HeaderMap::new)
}
