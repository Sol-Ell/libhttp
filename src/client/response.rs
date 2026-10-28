use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

use crate::body::{BodyChannel, BodyReader};
use crate::header::{AsHeaderName, Header, HeaderMap, HeaderRef};
use crate::{StatusCode, Version};

/// A response returned by a client verb.
pub struct ClientResponse {
    pub(crate) status: StatusCode,
    pub(crate) version: Version,
    pub(crate) headers: HeaderMap,
    pub(crate) body: BodyReader,
    pub(crate) channel: Arc<BodyChannel>,
    pub(crate) trailers: Arc<OnceLock<HeaderMap>>,
    pub(crate) peer: Option<SocketAddr>,
    pub(crate) local: Option<SocketAddr>,
    pub(crate) secure: bool,
}

impl ClientResponse {
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    pub fn header<T: Header>(&self) -> Option<&T> {
        self.headers.get::<T>()
    }

    pub fn headers_of<T: Header>(&self) -> impl Iterator<Item = &T> {
        self.headers.get_all::<T>()
    }

    pub fn header_value(&self, name: impl AsHeaderName) -> Option<HeaderRef<'_>> {
        self.headers.get_raw(name)
    }

    pub fn body(&mut self) -> &mut BodyReader {
        &mut self.body
    }

    pub fn trailers(&self) -> Option<&HeaderMap> {
        self.trailers.get()
    }

    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local
    }

    pub fn is_secure(&self) -> bool {
        self.secure
    }

    pub fn is_canceled(&self) -> bool {
        self.channel.available() == 0 && !self.channel.is_eof() && self.channel.is_closed()
    }
}

impl Drop for ClientResponse {
    fn drop(&mut self) {
        if !self.body.is_end() {
            self.channel.close();
        }
    }
}

impl std::fmt::Debug for ClientResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientResponse")
            .field("status", &self.status)
            .field("version", &self.version)
            .field("headers", &self.headers)
            .finish()
    }
}
