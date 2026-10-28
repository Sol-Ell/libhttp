use crate::{Method, ProtocolError, ProtocolErrorKind, Version};

/// A parsed HTTP request target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target<'a> {
    /// Origin-form: `/path?query`.
    Origin {
        raw: &'a [u8],
        path: &'a [u8],
        query: Option<&'a [u8]>,
    },
    /// Absolute-form: `scheme://authority/path?query`.
    Absolute {
        raw: &'a [u8],
        scheme: &'a [u8],
        authority: &'a [u8],
        path: &'a [u8],
        query: Option<&'a [u8]>,
    },
    /// Authority-form, used by `CONNECT`.
    Authority(&'a [u8]),
    /// Asterisk-form, used by `OPTIONS *`.
    Asterisk,
}

impl<'a> Target<'a> {
    /// Parses a target without method-specific restrictions.
    pub fn parse(bytes: &'a [u8]) -> Result<Self, ProtocolError> {
        Self::parse_inner(bytes, Version::Http11)
    }

    /// Parses a target with the restrictions of a request method.
    pub fn parse_for_request(
        bytes: &'a [u8],
        method: &Method,
        version: Version,
    ) -> Result<Self, ProtocolError> {
        let target = Self::parse_inner(bytes, version)?;
        match (&target, method) {
            (Self::Asterisk, Method::Options) => Ok(target),
            (Self::Asterisk, _) => Err(ProtocolError::new(
                version,
                ProtocolErrorKind::MalformedMessage,
                "asterisk target is only valid for OPTIONS",
            )),
            (Self::Authority(_), Method::Connect) => Ok(target),
            (Self::Authority(_), _) => Err(ProtocolError::new(
                version,
                ProtocolErrorKind::MalformedMessage,
                "authority target is only valid for CONNECT",
            )),
            (Self::Origin { .. } | Self::Absolute { .. }, Method::Connect) => {
                Err(ProtocolError::new(
                    version,
                    ProtocolErrorKind::MalformedMessage,
                    "CONNECT requires an authority target",
                ))
            }
            _ => Ok(target),
        }
    }

    /// The raw target bytes.
    pub fn as_bytes(&self) -> &'a [u8] {
        match self {
            Self::Origin { raw, .. } | Self::Absolute { raw, .. } => raw,
            Self::Authority(raw) => raw,
            Self::Asterisk => b"*",
        }
    }

    /// The path bytes, or an empty slice for authority/asterisk forms.
    pub fn path(&self) -> &'a [u8] {
        match self {
            Self::Origin { path, .. } | Self::Absolute { path, .. } => path,
            Self::Authority(_) | Self::Asterisk => b"",
        }
    }

    /// The query bytes, without the leading `?`.
    pub fn query(&self) -> Option<&'a [u8]> {
        match self {
            Self::Origin { query, .. } | Self::Absolute { query, .. } => *query,
            Self::Authority(_) | Self::Asterisk => None,
        }
    }

    /// The authority bytes for absolute or authority forms.
    pub fn authority(&self) -> Option<&'a [u8]> {
        match self {
            Self::Absolute { authority, .. } => Some(authority),
            Self::Authority(authority) => Some(authority),
            Self::Origin { .. } | Self::Asterisk => None,
        }
    }

    /// The scheme bytes for absolute form.
    pub fn scheme(&self) -> Option<&'a [u8]> {
        match self {
            Self::Absolute { scheme, .. } => Some(scheme),
            _ => None,
        }
    }

    /// Whether this is origin-form.
    pub fn is_origin_form(&self) -> bool {
        matches!(self, Self::Origin { .. })
    }

    /// Whether this is absolute-form.
    pub fn is_absolute_form(&self) -> bool {
        matches!(self, Self::Absolute { .. })
    }

    /// Whether this is authority-form.
    pub fn is_authority_form(&self) -> bool {
        matches!(self, Self::Authority(_))
    }

    /// Whether this is asterisk-form.
    pub fn is_asterisk(&self) -> bool {
        matches!(self, Self::Asterisk)
    }

    fn parse_inner(bytes: &'a [u8], version: Version) -> Result<Self, ProtocolError> {
        if bytes.is_empty() {
            return Err(ProtocolError::new(
                version,
                ProtocolErrorKind::MalformedMessage,
                "empty request target",
            ));
        }
        if bytes
            .iter()
            .any(|byte| *byte <= b' ' || *byte == 0x7f || *byte == b'#')
        {
            return Err(ProtocolError::new(
                version,
                ProtocolErrorKind::MalformedMessage,
                "request target contains a forbidden byte",
            ));
        }
        if bytes == b"*" {
            return Ok(Self::Asterisk);
        }
        if bytes[0] == b'/' {
            let (path, query) = split_query(bytes);
            return Ok(Self::Origin {
                raw: bytes,
                path,
                query,
            });
        }
        if let Some(scheme_end) = find_scheme_separator(bytes) {
            let scheme = &bytes[..scheme_end];
            if !matches!(scheme, b"http" | b"https") {
                return Err(ProtocolError::new(
                    version,
                    ProtocolErrorKind::Unsupported,
                    "unsupported URI scheme",
                ));
            }
            let rest = &bytes[scheme_end + 3..];
            if rest.is_empty() {
                return Err(ProtocolError::new(
                    version,
                    ProtocolErrorKind::MalformedMessage,
                    "absolute target has no authority",
                ));
            }
            let (authority, path_and_query) = match rest.iter().position(|byte| *byte == b'/') {
                Some(slash) => (&rest[..slash], &rest[slash..]),
                None => (rest, b"/" as &'a [u8]),
            };
            if authority.is_empty() || authority.iter().any(|byte| matches!(byte, b'?' | b'#')) {
                return Err(ProtocolError::new(
                    version,
                    ProtocolErrorKind::MalformedMessage,
                    "invalid absolute-form authority",
                ));
            }
            let (path, query) = split_query(path_and_query);
            return Ok(Self::Absolute {
                raw: bytes,
                scheme,
                authority,
                path,
                query,
            });
        }
        if bytes.iter().any(|byte| matches!(byte, b'/' | b'?' | b'#')) {
            return Err(ProtocolError::new(
                version,
                ProtocolErrorKind::MalformedMessage,
                "invalid request target",
            ));
        }
        Ok(Self::Authority(bytes))
    }
}

fn split_query(bytes: &[u8]) -> (&[u8], Option<&[u8]>) {
    match bytes.iter().position(|byte| *byte == b'?') {
        Some(question) => (&bytes[..question], Some(&bytes[question + 1..])),
        None => (bytes, None),
    }
}

fn find_scheme_separator(bytes: &[u8]) -> Option<usize> {
    let end = bytes.windows(3).position(|window| window == b"://")?;
    if end > 0
        && bytes[..end]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
    {
        Some(end)
    } else {
        None
    }
}

/// An owned, validated HTTP request target.
///
/// This is the owned counterpart of [`Target`]; it is used where a target must outlive the
/// request buffers, such as [`crate::server::DeferredFailure`]. The raw bytes are preserved
/// exactly as received.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct OwnedTarget {
    raw: Box<[u8]>,
}

impl OwnedTarget {
    /// Validates and owns a request target.
    pub fn parse(bytes: &[u8]) -> Result<Self, ProtocolError> {
        Target::parse(bytes)?;
        Ok(Self::from_validated(bytes.to_vec()))
    }

    pub(crate) fn from_validated(bytes: Vec<u8>) -> Self {
        Self {
            raw: bytes.into_boxed_slice(),
        }
    }

    /// The raw target bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.raw
    }

    /// The target as a UTF-8 string, if it is valid UTF-8.
    pub fn to_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.raw)
    }

    /// Re-parses the owned bytes as a borrowed target.
    pub fn as_target(&self) -> Result<Target<'_>, ProtocolError> {
        Target::parse(&self.raw)
    }

    /// The path bytes, or an empty slice if the stored target is not origin/absolute form.
    pub fn path(&self) -> &[u8] {
        self.as_target()
            .map(|target| target.path())
            .unwrap_or_default()
    }

    /// The query bytes, without the leading `?`.
    pub fn query(&self) -> Option<&[u8]> {
        self.as_target().ok().and_then(|target| target.query())
    }

    /// The authority bytes for absolute or authority forms.
    pub fn authority(&self) -> Option<&[u8]> {
        self.as_target().ok().and_then(|target| target.authority())
    }

    /// The scheme bytes for absolute form.
    pub fn scheme(&self) -> Option<&[u8]> {
        self.as_target().ok().and_then(|target| target.scheme())
    }

    /// Whether this is origin-form.
    pub fn is_origin_form(&self) -> bool {
        self.as_target().is_ok_and(|target| target.is_origin_form())
    }

    /// Whether this is absolute-form.
    pub fn is_absolute_form(&self) -> bool {
        self.as_target()
            .is_ok_and(|target| target.is_absolute_form())
    }

    /// Whether this is authority-form.
    pub fn is_authority_form(&self) -> bool {
        self.as_target()
            .is_ok_and(|target| target.is_authority_form())
    }

    /// Whether this is asterisk-form.
    pub fn is_asterisk(&self) -> bool {
        self.as_target().is_ok_and(|target| target.is_asterisk())
    }
}

impl std::fmt::Debug for OwnedTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("OwnedTarget")
            .field(&String::from_utf8_lossy(&self.raw))
            .finish()
    }
}

impl std::fmt::Display for OwnedTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&String::from_utf8_lossy(&self.raw))
    }
}

impl AsRef<[u8]> for OwnedTarget {
    fn as_ref(&self) -> &[u8] {
        &self.raw
    }
}

impl<'a> From<Target<'a>> for OwnedTarget {
    fn from(target: Target<'a>) -> Self {
        Self::from_validated(target.as_bytes().to_vec())
    }
}

impl<'a> Target<'a> {
    /// Copies this parsed target into an owned, validated value.
    pub fn to_owned_target(self) -> OwnedTarget {
        OwnedTarget::from(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_form() {
        let target = Target::parse(b"/a/b?x=1").unwrap();
        assert!(target.is_origin_form());
        assert_eq!(target.path(), b"/a/b");
        assert_eq!(target.query(), Some(&b"x=1"[..]));
        assert_eq!(target.as_bytes(), b"/a/b?x=1");
    }

    #[test]
    fn absolute_form() {
        let target = Target::parse(b"http://example.com:8080/a?b=c").unwrap();
        assert!(target.is_absolute_form());
        assert_eq!(target.scheme(), Some(&b"http"[..]));
        assert_eq!(target.authority(), Some(&b"example.com:8080"[..]));
        assert_eq!(target.path(), b"/a");
        assert_eq!(target.query(), Some(&b"b=c"[..]));
    }

    #[test]
    fn asterisk_and_authority_forms() {
        assert!(Target::parse(b"*").unwrap().is_asterisk());
        let connect = Target::parse(b"example.com:443").unwrap();
        assert!(connect.is_authority_form());
        assert_eq!(connect.authority(), Some(&b"example.com:443"[..]));
    }

    #[test]
    fn method_restrictions() {
        assert!(Target::parse_for_request(b"*", &Method::GET, Version::Http11).is_err());
        assert!(Target::parse_for_request(b"*", &Method::OPTIONS, Version::Http11).is_ok());
        assert!(
            Target::parse_for_request(b"example.com:443", &Method::CONNECT, Version::Http11)
                .is_ok()
        );
    }

    #[test]
    fn owned_target_preserves_bytes_and_views() {
        let owned = OwnedTarget::parse(b"/a/b?x=1").unwrap();
        assert_eq!(owned.as_bytes(), b"/a/b?x=1");
        assert_eq!(owned.path(), b"/a/b");
        assert_eq!(owned.query(), Some(&b"x=1"[..]));
        assert!(owned.is_origin_form());
        assert_eq!(
            Target::parse(b"/a").unwrap().to_owned_target().path(),
            b"/a"
        );

        let absolute = OwnedTarget::parse(b"http://example.com:8080/x").unwrap();
        assert_eq!(absolute.scheme(), Some(&b"http"[..]));
        assert_eq!(absolute.authority(), Some(&b"example.com:8080"[..]));
        assert!(absolute.is_absolute_form());
    }

    #[test]
    fn owned_target_rejects_invalid_bytes() {
        assert!(OwnedTarget::parse(b"").is_err());
        assert!(OwnedTarget::parse(b"ftp://example.com/").is_err());
    }

    #[test]
    fn forbidden_bytes_and_schemes_are_rejected() {
        assert!(Target::parse(b"/a b").is_err());
        assert!(Target::parse(b"/a\r\n").is_err());
        assert!(Target::parse(b"ftp://example.com/").is_err());
        assert!(Target::parse(b"").is_err());
    }
}
