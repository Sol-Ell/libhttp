use std::fmt;
use std::str::FromStr;

use super::version::Version;
use crate::error::{ProtocolError, ProtocolErrorKind};

/// An HTTP request method.
///
/// The known methods are represented as variants; an unknown but valid token is
/// preserved verbatim in [`Method::Other`]. Method names are case-sensitive.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Method {
    Get,
    Head,
    Post,
    Put,
    Delete,
    Connect,
    Options,
    Trace,
    Patch,
    Other(MethodExt),
}

impl Method {
    pub const GET: Method = Method::Get;
    pub const HEAD: Method = Method::Head;
    pub const POST: Method = Method::Post;
    pub const PUT: Method = Method::Put;
    pub const DELETE: Method = Method::Delete;
    pub const CONNECT: Method = Method::Connect;
    pub const OPTIONS: Method = Method::Options;
    pub const TRACE: Method = Method::Trace;
    pub const PATCH: Method = Method::Patch;

    /// Parse and validate an HTTP method token.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ProtocolError> {
        if !is_token(bytes) {
            return Err(malformed_method());
        }
        Ok(match bytes {
            b"GET" => Method::Get,
            b"HEAD" => Method::Head,
            b"POST" => Method::Post,
            b"PUT" => Method::Put,
            b"DELETE" => Method::Delete,
            b"CONNECT" => Method::Connect,
            b"OPTIONS" => Method::Options,
            b"TRACE" => Method::Trace,
            b"PATCH" => Method::Patch,
            other => Method::Other(MethodExt::new(other)),
        })
    }

    /// The wire token for this method.
    pub fn as_str(&self) -> &str {
        match self {
            Method::Get => "GET",
            Method::Head => "HEAD",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Delete => "DELETE",
            Method::Connect => "CONNECT",
            Method::Options => "OPTIONS",
            Method::Trace => "TRACE",
            Method::Patch => "PATCH",
            Method::Other(ext) => ext.as_str(),
        }
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Method").field(&self.as_str()).finish()
    }
}

impl FromStr for Method {
    type Err = ProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Method::from_bytes(value.as_bytes())
    }
}

/// An application-defined method token, preserved as sent.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct MethodExt {
    token: Box<str>,
}

impl MethodExt {
    /// Parse and validate an extension method token.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ProtocolError> {
        if !is_token(bytes) {
            return Err(malformed_method());
        }
        Ok(MethodExt::new(bytes))
    }

    pub fn as_str(&self) -> &str {
        &self.token
    }

    fn new(bytes: &[u8]) -> Self {
        // `is_token` only admits ASCII bytes, so this conversion cannot fail.
        let token = std::str::from_utf8(bytes)
            .expect("validated method tokens are ASCII")
            .to_owned()
            .into_boxed_str();
        MethodExt { token }
    }
}

impl fmt::Display for MethodExt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for MethodExt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("MethodExt").field(&self.as_str()).finish()
    }
}

fn malformed_method() -> ProtocolError {
    ProtocolError::new(Version::Http11, ProtocolErrorKind::MalformedMessage)
}

/// RFC 9110 `token`: 1*tchar.
fn is_token(bytes: &[u8]) -> bool {
    !bytes.is_empty() && bytes.iter().all(|&byte| is_tchar(byte))
}

fn is_tchar(byte: u8) -> bool {
    matches!(
        byte,
        b'!' | b'#'
            | b'$'
            | b'%'
            | b'&'
            | b'\''
            | b'*'
            | b'+'
            | b'-'
            | b'.'
            | b'^'
            | b'_'
            | b'`'
            | b'|'
            | b'~'
    ) || byte.is_ascii_alphanumeric()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_token_roundtrips() {
        assert_eq!(Method::GET.as_str(), "GET");
        assert_eq!(Method::from_bytes(b"GET").unwrap(), Method::Get);
        assert_eq!(Method::POST, Method::Post);
        assert_eq!(Method::DELETE.as_str(), "DELETE");
        assert_eq!(Method::PATCH.as_str(), "PATCH");
        assert!(Method::from_bytes(b"BAD METHOD").is_err());
        assert!(Method::from_bytes(b"").is_err());
    }

    #[test]
    fn method_keeps_unknown_tokens_and_case() {
        assert_eq!(Method::from_bytes(b"PURGE").unwrap().as_str(), "PURGE");
        assert!(matches!(
            Method::from_bytes(b"get").unwrap(),
            Method::Other(_)
        ));
        assert_eq!(Method::from_bytes(b"get").unwrap().as_str(), "get");
        assert_eq!(
            Method::from_bytes(b"M-SEARCH").unwrap().as_str(),
            "M-SEARCH"
        );
        assert_eq!(Method::CONNECT.as_str(), "CONNECT");
    }

    #[test]
    fn method_display_uses_the_token() {
        assert_eq!(Method::OPTIONS.to_string(), "OPTIONS");
        assert_eq!(Method::from_bytes(b"BREW").unwrap().to_string(), "BREW");
    }
}
