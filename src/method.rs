use std::fmt;

/// An HTTP request method.
///
/// Known methods are matched case-sensitively (`GET`, not `get`). Unknown but syntactically
/// valid tokens are preserved in [`Method::Other`].
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

/// An owned, syntactically valid custom method token.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct MethodExt(Box<str>);

/// The error returned when a byte slice is not a valid method token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidMethod;

impl Method {
    pub const GET: Self = Self::Get;
    pub const HEAD: Self = Self::Head;
    pub const POST: Self = Self::Post;
    pub const PUT: Self = Self::Put;
    pub const DELETE: Self = Self::Delete;
    pub const CONNECT: Self = Self::Connect;
    pub const OPTIONS: Self = Self::Options;
    pub const TRACE: Self = Self::Trace;
    pub const PATCH: Self = Self::Patch;

    /// Returns the method token as a string slice.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Delete => "DELETE",
            Self::Connect => "CONNECT",
            Self::Options => "OPTIONS",
            Self::Trace => "TRACE",
            Self::Patch => "PATCH",
            Self::Other(method) => &method.0,
        }
    }

    /// Parses a method token. Known methods are case-sensitive.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, InvalidMethod> {
        match bytes {
            b"GET" => return Ok(Self::Get),
            b"HEAD" => return Ok(Self::Head),
            b"POST" => return Ok(Self::Post),
            b"PUT" => return Ok(Self::Put),
            b"DELETE" => return Ok(Self::Delete),
            b"CONNECT" => return Ok(Self::Connect),
            b"OPTIONS" => return Ok(Self::Options),
            b"TRACE" => return Ok(Self::Trace),
            b"PATCH" => return Ok(Self::Patch),
            _ => {}
        }
        if bytes.is_empty() || !bytes.iter().all(|byte| is_token_byte(*byte)) {
            return Err(InvalidMethod);
        }
        let token = std::str::from_utf8(bytes).map_err(|_| InvalidMethod)?;
        Ok(Self::Other(MethodExt(token.into())))
    }

    /// Whether repeating this method is safe for an automatic retry.
    pub fn is_idempotent(&self) -> bool {
        matches!(
            self,
            Self::Get | Self::Head | Self::Put | Self::Delete | Self::Options | Self::Trace
        )
    }

    /// Whether the method is defined as safe by RFC 9110.
    pub fn is_safe(&self) -> bool {
        matches!(self, Self::Get | Self::Head | Self::Options | Self::Trace)
    }

    /// Whether this is the `CONNECT` method.
    pub fn is_connect(&self) -> bool {
        matches!(self, Self::Connect)
    }
}

impl MethodExt {
    /// Returns the custom method token.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Method({})", self.as_str())
    }
}

impl fmt::Debug for MethodExt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MethodExt({:?})", self.0)
    }
}

impl fmt::Display for InvalidMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid HTTP method token")
    }
}

impl std::error::Error for InvalidMethod {}

impl std::str::FromStr for Method {
    type Err = InvalidMethod;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_bytes(s.as_bytes())
    }
}

fn is_token_byte(byte: u8) -> bool {
    matches!(byte,
        b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.' |
        b'^' | b'_' | b'`' | b'|' | b'~' |
        b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_methods_are_case_sensitive() {
        assert_eq!(Method::from_bytes(b"GET").unwrap(), Method::Get);
        assert_eq!(Method::from_bytes(b"get").unwrap().as_str(), "get");
        assert!(matches!(
            Method::from_bytes(b"get").unwrap(),
            Method::Other(_)
        ));
    }

    #[test]
    fn invalid_methods_are_rejected() {
        assert!(Method::from_bytes(b"").is_err());
        assert!(Method::from_bytes(b"BAD METHOD").is_err());
        assert!(Method::from_bytes(b"BAD\nMETHOD").is_err());
        assert!(Method::from_bytes(b"\xff").is_err());
    }

    #[test]
    fn custom_tokens_round_trip() {
        let method = Method::from_bytes(b"PURGE").unwrap();
        assert_eq!(method.to_string(), "PURGE");
        assert!(!Method::from_bytes(b"X").unwrap().is_idempotent());
    }

    #[test]
    fn idempotent_set_is_as_documented() {
        for method in [
            Method::Get,
            Method::Head,
            Method::Put,
            Method::Delete,
            Method::Options,
            Method::Trace,
        ] {
            assert!(method.is_idempotent(), "{method}");
        }
        assert!(!Method::Post.is_idempotent());
    }
}
