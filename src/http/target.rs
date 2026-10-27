use std::fmt;
use std::str::FromStr;

use super::version::Version;
use crate::error::{ProtocolError, ProtocolErrorKind};

/// A validated request-target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target<'a> {
    /// Origin-form: path (beginning with `/`) plus an optional query.
    Origin {
        path: &'a [u8],
        query: Option<&'a [u8]>,
    },
    /// Absolute-form: scheme, authority, path and optional query.
    Absolute {
        scheme: &'a [u8],
        authority: &'a [u8],
        path: &'a [u8],
        query: Option<&'a [u8]>,
    },
    /// Authority-form, used by CONNECT.
    Authority(&'a [u8]),
    /// The asterisk-form target.
    Asterisk,
}

impl<'a> Target<'a> {
    /// Classify and validate one of the four RFC 9112 request-target forms.
    pub fn parse(bytes: &'a [u8]) -> Result<Self, ProtocolError> {
        if bytes.is_empty() || bytes.iter().any(|&byte| invalid_target_byte(byte)) {
            return Err(malformed_target());
        }
        if bytes == b"*" {
            return Ok(Target::Asterisk);
        }
        if bytes[0] == b'/' {
            let (path, query) = split_query(bytes);
            return Ok(Target::Origin { path, query });
        }
        if let Some(separator) = find_subslice(bytes, b"://") {
            let scheme = &bytes[..separator];
            if !is_scheme(scheme) {
                return Err(malformed_target());
            }
            let rest = &bytes[separator + 3..];
            let authority_end = rest
                .iter()
                .position(|&byte| byte == b'/' || byte == b'?')
                .unwrap_or(rest.len());
            let authority = &rest[..authority_end];
            if !is_authority(authority) {
                return Err(malformed_target());
            }
            let remainder = &rest[authority_end..];
            let (path, query) = if remainder.is_empty() {
                (&b"/"[..], None)
            } else if remainder[0] == b'?' {
                (&b"/"[..], Some(&remainder[1..]))
            } else {
                split_query(remainder)
            };
            return Ok(Target::Absolute {
                scheme,
                authority,
                path,
                query,
            });
        }
        if !is_authority(bytes) {
            return Err(malformed_target());
        }
        Ok(Target::Authority(bytes))
    }

    pub fn path(self) -> Option<&'a [u8]> {
        match self {
            Target::Origin { path, .. } | Target::Absolute { path, .. } => Some(path),
            Target::Authority(_) | Target::Asterisk => None,
        }
    }

    pub fn query(self) -> Option<&'a [u8]> {
        match self {
            Target::Origin { query, .. } | Target::Absolute { query, .. } => query,
            Target::Authority(_) | Target::Asterisk => None,
        }
    }

    pub fn scheme(self) -> Option<&'a [u8]> {
        match self {
            Target::Absolute { scheme, .. } => Some(scheme),
            _ => None,
        }
    }

    pub fn authority(self) -> Option<&'a [u8]> {
        match self {
            Target::Absolute { authority, .. } => Some(authority),
            Target::Authority(authority) => Some(authority),
            Target::Origin { .. } | Target::Asterisk => None,
        }
    }

    pub fn is_asterisk(self) -> bool {
        matches!(self, Target::Asterisk)
    }
}

/// An owned absolute URI, used by the client API.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Uri {
    scheme: String,
    authority: String,
    target: String,
}

impl Uri {
    /// Parse an absolute `scheme://authority[path][?query]` URI. Fragments and
    /// userinfo are rejected.
    pub fn parse(input: &str) -> Result<Self, ProtocolError> {
        let bytes = input.as_bytes();
        if bytes.is_empty() || bytes.iter().any(|&byte| invalid_target_byte(byte)) {
            return Err(malformed_target());
        }
        let separator = find_subslice(bytes, b"://").ok_or_else(malformed_target)?;
        let scheme = &input[..separator];
        if !is_scheme(scheme.as_bytes()) {
            return Err(malformed_target());
        }
        let rest = &input[separator + 3..];
        let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
        let authority = &rest[..authority_end];
        if !is_authority(authority.as_bytes()) {
            return Err(malformed_target());
        }
        let remainder = &rest[authority_end..];
        let target = if remainder.is_empty() {
            "/".to_owned()
        } else if remainder.starts_with('?') {
            format!("/{remainder}")
        } else {
            remainder.to_owned()
        };
        Ok(Uri {
            scheme: scheme.to_owned(),
            authority: authority.to_owned(),
            target,
        })
    }

    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// Host and port, exactly as they appeared in the authority.
    pub fn authority(&self) -> &str {
        &self.authority
    }

    pub fn host(&self) -> &str {
        if let Some(rest) = self.authority.strip_prefix('[') {
            return rest
                .split_once(']')
                .map_or(self.authority.as_str(), |(host, _)| host);
        }
        match self.authority.rsplit_once(':') {
            Some((host, port)) if is_port(port.as_bytes()) => host,
            _ => self.authority.as_str(),
        }
    }

    pub fn port(&self) -> Option<u16> {
        let port = if let Some(rest) = self.authority.strip_prefix('[') {
            rest.split_once(']')?.1.strip_prefix(':')?
        } else {
            self.authority.rsplit_once(':')?.1
        };
        port.parse().ok()
    }

    pub fn path(&self) -> &str {
        self.target
            .split_once('?')
            .map_or(self.target.as_str(), |(path, _)| path)
    }

    pub fn query(&self) -> Option<&str> {
        self.target.split_once('?').map(|(_, query)| query)
    }

    pub fn path_and_query(&self) -> &str {
        &self.target
    }
}

impl fmt::Display for Uri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}{}", self.scheme, self.authority, self.target)
    }
}

impl FromStr for Uri {
    type Err = ProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uri::parse(value)
    }
}

impl TryFrom<&str> for Uri {
    type Error = ProtocolError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Uri::parse(value)
    }
}

fn malformed_target() -> ProtocolError {
    ProtocolError::new(Version::Http11, ProtocolErrorKind::MalformedMessage)
}

fn invalid_target_byte(byte: u8) -> bool {
    byte <= 0x20 || byte == 0x7f || byte == b'#'
}

fn split_query(bytes: &[u8]) -> (&[u8], Option<&[u8]>) {
    match bytes.iter().position(|&byte| byte == b'?') {
        Some(position) => (&bytes[..position], Some(&bytes[position + 1..])),
        None => (bytes, None),
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn is_scheme(bytes: &[u8]) -> bool {
    match bytes.split_first() {
        Some((first, rest)) => {
            first.is_ascii_alphabetic()
                && rest
                    .iter()
                    .all(|&byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
        }
        None => false,
    }
}

fn is_authority(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return false;
    }
    if bytes
        .iter()
        .any(|&byte| byte <= 0x20 || byte >= 0x7f || matches!(byte, b'/' | b'?' | b'#' | b'@'))
    {
        return false;
    }
    if bytes[0] == b'[' {
        let Some(close) = bytes.iter().position(|&byte| byte == b']') else {
            return false;
        };
        if close == 1 {
            return false;
        }
        if !bytes[1..close]
            .iter()
            .all(|&byte| is_host_byte(byte) || byte == b':')
        {
            return false;
        }
        let rest = &bytes[close + 1..];
        return rest.is_empty() || (rest[0] == b':' && is_port(&rest[1..]));
    }
    let (host, port) = match bytes.iter().rposition(|&byte| byte == b':') {
        Some(position) => (&bytes[..position], Some(&bytes[position + 1..])),
        None => (bytes, None),
    };
    !host.is_empty() && host.iter().all(|&byte| is_host_byte(byte)) && port.is_none_or(is_port)
}

fn is_host_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'%')
}

fn is_port(bytes: &[u8]) -> bool {
    !bytes.is_empty()
        && bytes.iter().all(|&byte| byte.is_ascii_digit())
        && std::str::from_utf8(bytes)
            .ok()
            .and_then(|port| port.parse::<u16>().ok())
            .is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_parses_origin_form() {
        let target = Target::parse(b"/a/b?x=1").unwrap();
        assert_eq!(target.path(), Some(&b"/a/b"[..]));
        assert_eq!(target.query(), Some(&b"x=1"[..]));
        assert_eq!(target.scheme(), None);
        assert_eq!(target.authority(), None);

        let bare = Target::parse(b"/").unwrap();
        assert_eq!(bare.path(), Some(&b"/"[..]));
        assert_eq!(bare.query(), None);

        let empty_query = Target::parse(b"/a?").unwrap();
        assert_eq!(empty_query.query(), Some(&b""[..]));

        assert!(Target::parse(b"").is_err());
        assert!(Target::parse(b"/bad\rpath").is_err());
        assert!(Target::parse(b"/bad path").is_err());
        assert!(Target::parse(b"/frag#ment").is_err());
    }

    #[test]
    fn target_parses_absolute_form() {
        let target = Target::parse(b"http://example.com:8080/x?y=2").unwrap();
        assert_eq!(target.scheme(), Some(&b"http"[..]));
        assert_eq!(target.authority(), Some(&b"example.com:8080"[..]));
        assert_eq!(target.path(), Some(&b"/x"[..]));
        assert_eq!(target.query(), Some(&b"y=2"[..]));

        let no_path = Target::parse(b"https://example.com").unwrap();
        assert_eq!(no_path.path(), Some(&b"/"[..]));
        assert_eq!(no_path.query(), None);

        assert!(Target::parse(b"http:///x").is_err());
        assert!(Target::parse(b"://example.com").is_err());
    }

    #[test]
    fn target_parses_authority_and_asterisk_forms() {
        let authority = Target::parse(b"example.com:443").unwrap();
        assert!(matches!(authority, Target::Authority(_)));
        assert_eq!(authority.authority(), Some(&b"example.com:443"[..]));
        assert_eq!(authority.path(), None);

        assert!(Target::parse(b"*").unwrap().is_asterisk());
        assert_eq!(Target::parse(b"*").unwrap().path(), None);
        assert!(Target::parse(b"**").is_err());
    }

    #[test]
    fn uri_parses_absolute_uris() {
        let uri = Uri::parse("https://example.com:8443/a/b?x=1").unwrap();
        assert_eq!(uri.scheme(), "https");
        assert_eq!(uri.authority(), "example.com:8443");
        assert_eq!(uri.host(), "example.com");
        assert_eq!(uri.port(), Some(8443));
        assert_eq!(uri.path(), "/a/b");
        assert_eq!(uri.query(), Some("x=1"));
        assert_eq!(uri.path_and_query(), "/a/b?x=1");
        assert_eq!(uri.to_string(), "https://example.com:8443/a/b?x=1");

        let bare = Uri::parse("http://example.com").unwrap();
        assert_eq!(bare.path(), "/");
        assert_eq!(bare.query(), None);
        assert_eq!(bare.port(), None);
        assert_eq!(bare.to_string(), "http://example.com/");

        let ipv6 = Uri::parse("http://[::1]:8080/x").unwrap();
        assert_eq!(ipv6.host(), "::1");
        assert_eq!(ipv6.port(), Some(8080));

        assert!(Uri::parse("not a uri").is_err());
        assert!(Uri::parse("http://").is_err());
        assert!(Uri::parse("http://example.com:notaport/").is_err());
        assert!(Uri::parse("http://example.com/a#frag").is_err());
        assert!(Uri::parse("1http://example.com/").is_err());
    }
}
