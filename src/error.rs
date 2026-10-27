use std::fmt;

use crate::http::{HeaderError, Version};

/// The closed error type used by the public API and by the runtime.
#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Timeout(TimeoutKind),
    Protocol(ProtocolError),
    Tls(TlsError),
    BodyTooLarge { limit: u64 },
    HeadTooLarge { limit: u32 },
    Overloaded,
    Canceled,
    Closed,
    InvalidRequest(&'static str),
    InvalidConfig(&'static str),
    Header(HeaderError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(error) => write!(f, "i/o error: {error}"),
            Error::Timeout(kind) => write!(f, "timeout: {kind}"),
            Error::Protocol(error) => write!(f, "protocol error: {error}"),
            Error::Tls(error) => write!(f, "{error}"),
            Error::BodyTooLarge { limit } => {
                write!(f, "body too large (limit {limit} bytes)")
            }
            Error::HeadTooLarge { limit } => {
                write!(f, "head too large (limit {limit} bytes)")
            }
            Error::Overloaded => f.write_str("overloaded"),
            Error::Canceled => f.write_str("canceled"),
            Error::Closed => f.write_str("closed"),
            Error::InvalidRequest(message) => write!(f, "invalid request: {message}"),
            Error::InvalidConfig(message) => write!(f, "invalid config: {message}"),
            Error::Header(error) => write!(f, "header error: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(error) => Some(error),
            Error::Protocol(error) => Some(error),
            Error::Tls(error) => Some(error),
            Error::Header(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::Io(error)
    }
}

impl From<TimeoutKind> for Error {
    fn from(kind: TimeoutKind) -> Self {
        Error::Timeout(kind)
    }
}

impl From<ProtocolError> for Error {
    fn from(error: ProtocolError) -> Self {
        Error::Protocol(error)
    }
}

impl From<HeaderError> for Error {
    fn from(error: HeaderError) -> Self {
        Error::Header(error)
    }
}

impl From<TlsError> for Error {
    fn from(error: TlsError) -> Self {
        Error::Tls(error)
    }
}

/// The operation that timed out.
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

impl TimeoutKind {
    /// A stable, lowercase name for logs and metrics.
    pub fn as_str(self) -> &'static str {
        match self {
            TimeoutKind::Connect => "connect",
            TimeoutKind::TlsHandshake => "tls_handshake",
            TimeoutKind::HeaderRead => "header_read",
            TimeoutKind::BodyIdle => "body_idle",
            TimeoutKind::ReadStall => "read_stall",
            TimeoutKind::WriteStall => "write_stall",
            TimeoutKind::PoolQueue => "pool_queue",
            TimeoutKind::Total => "total",
            TimeoutKind::Deferred => "deferred",
        }
    }
}

impl fmt::Display for TimeoutKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A typed protocol failure with a stable classification.
#[derive(Debug)]
pub struct ProtocolError {
    version: Version,
    kind: ProtocolErrorKind,
}

impl ProtocolError {
    pub fn new(version: Version, kind: ProtocolErrorKind) -> Self {
        ProtocolError { version, kind }
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn kind(&self) -> ProtocolErrorKind {
        self.kind
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.version, self.kind)
    }
}

impl std::error::Error for ProtocolError {}

/// The stable classification of a [`ProtocolError`].
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

impl ProtocolErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ProtocolErrorKind::MalformedMessage => "malformed message",
            ProtocolErrorKind::Framing => "framing",
            ProtocolErrorKind::HeaderCompression => "header compression",
            ProtocolErrorKind::FlowControl => "flow control",
            ProtocolErrorKind::StreamState => "stream state",
            ProtocolErrorKind::Settings => "settings",
            ProtocolErrorKind::Trailer => "trailer",
            ProtocolErrorKind::Unsupported => "unsupported",
        }
    }
}

impl fmt::Display for ProtocolErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// TLS failure. A stub until the M3 TLS milestone fills in the variants.
#[derive(Debug)]
pub struct TlsError {
    message: &'static str,
}

impl TlsError {
    /// The M3 TLS milestone will construct these from real handshake errors.
    #[allow(dead_code)]
    pub(crate) fn new(message: &'static str) -> Self {
        TlsError { message }
    }

    pub fn message(&self) -> &'static str {
        self.message
    }
}

impl fmt::Display for TlsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TLS error: {}", self.message)
    }
}

impl std::error::Error for TlsError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::{HeaderError, Version};

    #[test]
    fn protocol_error_exposes_version_and_kind() {
        let error = ProtocolError::new(Version::Http11, ProtocolErrorKind::Framing);
        assert_eq!(error.version(), Version::Http11);
        assert_eq!(error.kind(), ProtocolErrorKind::Framing);
        assert_eq!(error.to_string(), "HTTP/1.1 framing");
    }

    #[test]
    fn timeout_kinds_are_classified() {
        assert_eq!(TimeoutKind::Connect.to_string(), "connect");
        assert_eq!(TimeoutKind::TlsHandshake.to_string(), "tls_handshake");
        assert_eq!(TimeoutKind::HeaderRead.to_string(), "header_read");
        assert_eq!(TimeoutKind::BodyIdle.to_string(), "body_idle");
        assert_eq!(TimeoutKind::ReadStall.to_string(), "read_stall");
        assert_eq!(TimeoutKind::WriteStall.to_string(), "write_stall");
        assert_eq!(TimeoutKind::PoolQueue.to_string(), "pool_queue");
        assert_eq!(TimeoutKind::Total.to_string(), "total");
        assert_eq!(TimeoutKind::Deferred.to_string(), "deferred");
    }

    #[test]
    fn error_displays_and_chains_sources() {
        let io = Error::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe"));
        assert!(io.to_string().starts_with("i/o error"));
        assert!(std::error::Error::source(&io).is_some());

        let header = Error::Header(HeaderError::decode("bad date"));
        assert_eq!(header.to_string(), "header error: bad date");
        assert!(std::error::Error::source(&header).is_some());

        let protocol = Error::Protocol(ProtocolError::new(
            Version::Http11,
            ProtocolErrorKind::MalformedMessage,
        ));
        assert_eq!(
            protocol.to_string(),
            "protocol error: HTTP/1.1 malformed message"
        );
        assert!(std::error::Error::source(&protocol).is_some());

        assert_eq!(
            Error::BodyTooLarge { limit: 8 }.to_string(),
            "body too large (limit 8 bytes)"
        );
        assert_eq!(
            Error::HeadTooLarge { limit: 16 }.to_string(),
            "head too large (limit 16 bytes)"
        );
        assert_eq!(Error::Overloaded.to_string(), "overloaded");
        assert_eq!(Error::Canceled.to_string(), "canceled");
        assert_eq!(Error::Closed.to_string(), "closed");
        assert_eq!(
            Error::InvalidRequest("bad host").to_string(),
            "invalid request: bad host"
        );
        assert_eq!(
            Error::InvalidConfig("zero workers").to_string(),
            "invalid config: zero workers"
        );
        assert_eq!(
            Error::Timeout(TimeoutKind::HeaderRead).to_string(),
            "timeout: header_read"
        );
    }

    #[test]
    fn tls_error_is_a_stub_with_display_and_source() {
        let error = TlsError::new("handshake failed");
        assert_eq!(error.to_string(), "TLS error: handshake failed");
        assert!(std::error::Error::source(&error).is_none());
        assert!(std::error::Error::source(&Error::Tls(error)).is_some());
    }

    #[test]
    fn error_conversions_are_available() {
        let io: Error = std::io::Error::other("boom").into();
        assert!(matches!(io, Error::Io(_)));
        let protocol: Error =
            ProtocolError::new(Version::Http10, ProtocolErrorKind::Unsupported).into();
        assert!(matches!(protocol, Error::Protocol(_)));
        let header: Error = HeaderError::invalid_name().into();
        assert!(matches!(header, Error::Header(_)));
        let timeout: Error = TimeoutKind::Deferred.into();
        assert!(matches!(timeout, Error::Timeout(TimeoutKind::Deferred)));
    }
}
