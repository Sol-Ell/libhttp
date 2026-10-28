use std::fmt;

/// The HTTP protocol version of a message.
///
/// v1.0 parses only HTTP/1.0 and HTTP/1.1. The HTTP/2 and HTTP/3 variants exist so callers can
/// classify values, but no builder or parser accepts them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Version {
    Http10,
    Http11,
    Http2,
    Http3,
}

/// The error returned for an unsupported or malformed version string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidVersion;

impl Version {
    /// Parses an HTTP/1.x version string. HTTP/2 and HTTP/3 are not accepted.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, InvalidVersion> {
        match bytes {
            b"HTTP/1.0" => Ok(Self::Http10),
            b"HTTP/1.1" => Ok(Self::Http11),
            _ => Err(InvalidVersion),
        }
    }

    /// Returns the wire spelling of the version.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http10 => "HTTP/1.0",
            Self::Http11 => "HTTP/1.1",
            Self::Http2 => "HTTP/2",
            Self::Http3 => "HTTP/3",
        }
    }

    /// Whether this is an HTTP/1.x version.
    pub const fn is_http1(self) -> bool {
        matches!(self, Self::Http10 | Self::Http11)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for InvalidVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unsupported HTTP version (v1.0 accepts HTTP/1.0 and HTTP/1.1)")
    }
}

impl std::error::Error for InvalidVersion {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_http1_is_parsed() {
        assert_eq!(Version::from_bytes(b"HTTP/1.1").unwrap(), Version::Http11);
        assert_eq!(Version::from_bytes(b"HTTP/1.0").unwrap(), Version::Http10);
        assert!(Version::from_bytes(b"HTTP/2").is_err());
        assert!(Version::from_bytes(b"HTTP/2.0").is_err());
        assert!(Version::from_bytes(b"HTTP/3").is_err());
        assert!(Version::from_bytes(b"http/1.1").is_err());
    }
}
