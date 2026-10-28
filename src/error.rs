use std::borrow::Cow;
use std::fmt;

use crate::header::HeaderError;
use crate::version::Version;

/// The kind of deadline that expired.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TimeoutKind {
    Connect,
    TlsHandshake,
    HeaderRead,
    BodyIdle,
    ReadStall,
    WriteStall,
    PoolQueue,
    Total,
    Deferred,
}

impl fmt::Display for TimeoutKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Connect => "connect",
            Self::TlsHandshake => "TLS handshake",
            Self::HeaderRead => "header read",
            Self::BodyIdle => "body idle",
            Self::ReadStall => "read stall",
            Self::WriteStall => "write stall",
            Self::PoolQueue => "pool queue",
            Self::Total => "total request",
            Self::Deferred => "deferred exchange",
        };
        f.write_str(name)
    }
}

/// A stable classification of a protocol failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProtocolErrorKind {
    MalformedMessage,
    Framing,
    HeaderCompression,
    FlowControl,
    StreamState,
    Settings,
    Trailer,
    Unsupported,
}

impl fmt::Display for ProtocolErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::MalformedMessage => "malformed message",
            Self::Framing => "framing",
            Self::HeaderCompression => "header compression",
            Self::FlowControl => "flow control",
            Self::StreamState => "stream state",
            Self::Settings => "settings",
            Self::Trailer => "trailer",
            Self::Unsupported => "unsupported",
        };
        f.write_str(name)
    }
}

/// A typed protocol failure with a stable classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{version} {kind}: {detail}")]
pub struct ProtocolError {
    version: Version,
    kind: ProtocolErrorKind,
    detail: &'static str,
}

impl ProtocolError {
    pub(crate) const fn new(
        version: Version,
        kind: ProtocolErrorKind,
        detail: &'static str,
    ) -> Self {
        Self {
            version,
            kind,
            detail,
        }
    }

    /// The version whose parser rejected the message.
    pub const fn version(self) -> Version {
        self.version
    }

    /// The stable failure category.
    pub const fn kind(self) -> ProtocolErrorKind {
        self.kind
    }

    /// A short, stable detail string.
    pub const fn detail(self) -> &'static str {
        self.detail
    }
}

/// A TLS failure.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct TlsError {
    message: Cow<'static, str>,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl TlsError {
    pub(crate) fn with_source(
        message: impl Into<Cow<'static, str>>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// The human-readable detail.
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// The library error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An operating-system or socket error.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// A deadline expired.
    #[error("timeout: {0}")]
    Timeout(TimeoutKind),

    /// The peer sent an invalid message.
    #[error("{0}")]
    Protocol(#[from] ProtocolError),

    /// A TLS handshake, record, or close-notify failure.
    #[error("TLS error: {0}")]
    Tls(#[from] TlsError),

    /// The decoded request body exceeded its configured limit.
    #[error("body too large (limit {limit} bytes)")]
    BodyTooLarge { limit: u64 },

    /// The message head exceeded its configured limit.
    #[error("head too large (limit {limit} bytes)")]
    HeadTooLarge { limit: u32 },

    /// No queue, worker, parking, or pool capacity is available.
    #[error("overloaded")]
    Overloaded,

    /// The exchange was canceled (peer reset, shutdown, or token).
    #[error("canceled")]
    Canceled,

    /// The handle or connection is closed.
    #[error("closed")]
    Closed,

    /// The application or caller passed an invalid request/response.
    #[error("invalid request: {0}")]
    InvalidRequest(&'static str),

    /// A builder or limit combination was rejected.
    #[error("invalid configuration: {0}")]
    InvalidConfig(&'static str),

    /// A typed header failed to decode, encode, or register.
    #[error("header error: {0}")]
    Header(#[from] HeaderError),
}

impl Error {
    pub(crate) const fn protocol(
        version: Version,
        kind: ProtocolErrorKind,
        detail: &'static str,
    ) -> Self {
        Self::Protocol(ProtocolError::new(version, kind, detail))
    }

    /// Whether this is a cancellation or a closed handle.
    pub fn is_canceled(&self) -> bool {
        matches!(self, Self::Canceled | Self::Closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_error_keeps_classification() {
        let error = Error::protocol(Version::Http11, ProtocolErrorKind::Framing, "bad chunk");
        match error {
            Error::Protocol(protocol) => {
                assert_eq!(protocol.version(), Version::Http11);
                assert_eq!(protocol.kind(), ProtocolErrorKind::Framing);
                assert!(protocol.to_string().contains("bad chunk"));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn tls_error_displays_message() {
        use std::error::Error as _;
        let error = TlsError::with_source("handshake failed", std::io::Error::other("inner"));
        assert_eq!(error.to_string(), "handshake failed");
        assert!(error.source().is_some());
    }
}
