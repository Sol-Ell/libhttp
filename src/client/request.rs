use std::time::Duration;

use crate::body::{IntoRequestBody, RequestBody};
use crate::header::{
    Header, HeaderError, HeaderMap, HeaderName, HeaderValue, IntoHeaderName, IntoHeaderValue,
};
use crate::{CancelToken, Error, Method, Uri};

/// A client request consumed by [`super::Client`] verbs.
pub struct ClientRequest {
    pub(crate) method: Method,
    pub(crate) uri: Uri,
    pub(crate) headers: HeaderMap,
    pub(crate) body: RequestBody,
    pub(crate) timeout: Option<Duration>,
    pub(crate) cancel: Option<CancelToken>,
}

impl ClientRequest {
    /// A `GET` request.
    pub fn get(uri: Uri) -> Self {
        Self::method(Method::Get, uri)
    }

    /// A `POST` request.
    pub fn post(uri: Uri) -> Self {
        Self::method(Method::Post, uri)
    }

    /// A request with an explicit method.
    pub fn method(method: Method, uri: Uri) -> Self {
        Self {
            method,
            uri,
            headers: HeaderMap::new(),
            body: RequestBody::default(),
            timeout: None,
            cancel: None,
        }
    }

    /// Replaces a typed request header.
    pub fn set<T: Header>(mut self, value: T) -> Self {
        self.headers.set(value);
        self
    }

    /// Appends a typed request header.
    pub fn append<T: Header>(mut self, value: T) -> Result<Self, HeaderError> {
        self.headers.append(value)?;
        Ok(self)
    }

    /// Alias for [`ClientRequest::set`] for typed headers.
    pub fn header<T: Header>(self, value: T) -> Self {
        self.set(value)
    }

    /// Adds a raw request header.
    pub fn raw_header(
        mut self,
        name: impl IntoHeaderName,
        value: impl IntoHeaderValue,
    ) -> Result<Self, Error> {
        self.headers.append_raw(name, value)?;
        Ok(self)
    }

    /// Sets a buffered request body.
    pub fn body(mut self, body: impl IntoRequestBody) -> Self {
        self.body = body.into_request_body();
        self
    }

    /// Sets the total request deadline.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Arms a cancellation token for this call.
    pub fn cancel_with(mut self, token: &CancelToken) -> Self {
        self.cancel = Some(token.clone());
        self
    }
}

impl std::fmt::Debug for ClientRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientRequest")
            .field("method", &self.method)
            .field("uri", &self.uri)
            .field("headers", &self.headers)
            .field("body_len", &self.body.len())
            .finish()
    }
}
