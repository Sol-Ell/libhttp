use std::fmt;

use crate::error::{ProtocolError, ProtocolErrorKind};

/// The HTTP protocol version of an exchange.
///
/// Only HTTP/1.0 and HTTP/1.1 are accepted by [`Version::from_bytes`] in this
/// milestone; the HTTP/2 and HTTP/3 variants exist for later protocol engines.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Version {
    Http10,
    #[default]
    Http11,
    Http2,
    Http3,
}

impl Version {
    /// Parse an HTTP/1.x version token. HTTP/0.9, HTTP/2 and HTTP/3 are not
    /// accepted here.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ProtocolError> {
        match bytes {
            b"HTTP/1.0" => Ok(Version::Http10),
            b"HTTP/1.1" => Ok(Version::Http11),
            _ => Err(ProtocolError::new(
                Version::Http11,
                ProtocolErrorKind::Unsupported,
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Version::Http10 => "HTTP/1.0",
            Version::Http11 => "HTTP/1.1",
            Version::Http2 => "HTTP/2",
            Version::Http3 => "HTTP/3",
        }
    }

    pub fn is_http1(self) -> bool {
        matches!(self, Version::Http10 | Version::Http11)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_accepts_http1_only() {
        assert_eq!(Version::from_bytes(b"HTTP/1.0").unwrap(), Version::Http10);
        assert_eq!(Version::from_bytes(b"HTTP/1.1").unwrap(), Version::Http11);
        assert_eq!(Version::Http11.as_str(), "HTTP/1.1");
        assert!(Version::Http10.is_http1());
        assert!(Version::Http11.is_http1());
        assert!(Version::from_bytes(b"HTTP/2.0").is_err());
        assert!(Version::from_bytes(b"HTTP/0.9").is_err());
        assert!(Version::from_bytes(b"http/1.1").is_err());
        assert!(Version::from_bytes(b"HTTP/1.10").is_err());
        assert!(Version::from_bytes(b"").is_err());
    }

    #[test]
    fn version_display() {
        assert_eq!(Version::Http10.to_string(), "HTTP/1.0");
        assert_eq!(Version::Http11.to_string(), "HTTP/1.1");
    }
}
