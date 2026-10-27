#![forbid(unsafe_op_in_unsafe_fn)]

mod error;
mod http;
mod rt;

pub use error::{Error, ProtocolError, ProtocolErrorKind, TimeoutKind, TlsError};
pub use http::{
    AsHeaderName, Header, HeaderError, HeaderMap, HeaderName, HeaderRef, HeaderValue,
    IntoHeaderName, IntoHeaderValue, Method, MethodExt, StatusCode, Target, Uri, Version,
};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_identity_is_libhttp() {
        assert_eq!(env!("CARGO_PKG_NAME"), "libhttp");
        assert_eq!(env!("CARGO_PKG_VERSION"), "0.1.0");
        assert_eq!(option_env!("CARGO_PKG_RUST_VERSION"), Some("1.85"));
    }
}
