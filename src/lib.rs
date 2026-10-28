//! `libhttp` — a synchronous HTTP/1.1 server and client library.
//!
//! There is no async runtime, `Future`, `Waker`, executor, or thread-per-connection in this
//! crate. Handlers are ordinary functions run on a bounded worker pool; reactor threads own
//! sockets and never run user code.
//!
//! The public API is frozen by `docs/DESIGN.md`. v1.0 implements HTTP/1.1 server and client,
//! TLS, typed headers, streaming bodies, and deferred completion. HTTP/2 and HTTP/3 are not
//! part of v1.0.

#![forbid(unsafe_op_in_unsafe_fn)]

#[cfg(all(not(feature = "server"), not(feature = "client")))]
compile_error!("libhttp requires at least one of the `server` or `client` features");

mod body;
mod cancel;
pub mod client;
mod config;
mod error;
mod h1;
pub mod header;
mod method;
mod net;
mod runtime;
pub mod server;
mod status;
mod target;
pub mod tls;
mod uri;
mod version;

pub use body::{
    BodyChunk, BodyReader, BodyWriter, IntoRequestBody, IntoResponseBody, RequestBody, ResponseBody,
};
pub use cancel::CancelToken;
pub use client::{
    Client, ClientBuilder, ClientHandle, ClientRequest, ClientResponse, ClientStats, PoolConfig,
};
pub use config::{Limits, Timeouts};
pub use error::{Error, ProtocolError, ProtocolErrorKind, TimeoutKind, TlsError};
pub use header::{
    Accept, AcceptEncoding, AcceptLanguage, AcceptRanges, Age, Allow, AsHeaderName, Authorization,
    CacheControl, Connection, ContentEncoding, ContentLanguage, ContentLength, ContentLocation,
    ContentRange, ContentType, Cookie, Date, ETag, Expect, Expires, Header, HeaderError,
    HeaderErrorKind, HeaderMap, HeaderName, HeaderRef, HeaderValue, Host, IfMatch, IfModifiedSince,
    IfNoneMatch, IfRange, IfUnmodifiedSince, IntoHeaderName, IntoHeaderValue, LastModified, Link,
    Location, Pragma, ProxyAuthenticate, ProxyAuthorization, Range, Referer, RetryAfter, SetCookie,
    Te, Trailer, TransferEncoding, UserAgent, Vary, Warning, WwwAuthenticate,
};
pub use method::{InvalidMethod, Method, MethodExt};
pub use runtime::{Runtime, RuntimeBuilder, RuntimeHandle, RuntimeStats};
pub use server::{Handler, Server, ServerBuilder, ServerHandle};
pub use status::{InvalidStatusCode, StatusCode};
pub use target::Target;
pub use tls::{TlsClientConfig, TlsServerConfig};
pub use uri::{InvalidUri, Uri};
pub use version::{InvalidVersion, Version};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_identity_is_libhttp() {
        assert_eq!(env!("CARGO_PKG_NAME"), "libhttp");
        assert_eq!(option_env!("CARGO_PKG_RUST_VERSION"), Some("1.85"));
    }
}
