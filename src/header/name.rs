use std::fmt;

use http::header::HeaderName as HttpHeaderName;

use super::HeaderError;

/// A validated, lowercase HTTP field name.
///
/// Names are normalized to lowercase and are cheap to clone. Known names are constants in
/// [`crate::header`]; custom names use [`HeaderName::from_bytes`] or [`HeaderName::from_static`].
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct HeaderName(HttpHeaderName);

impl HeaderName {
    /// Creates a name from a lowercase static string.
    ///
    /// # Panics
    ///
    /// Panics at compile time if `src` is not a valid lowercase HTTP token. Use
    /// [`HeaderName::from_bytes`] for runtime input.
    pub const fn from_static(src: &'static str) -> Self {
        Self(HttpHeaderName::from_static(src))
    }

    /// Parses a header name, normalizing ASCII uppercase to lowercase.
    pub fn from_bytes(src: &[u8]) -> Result<Self, HeaderError> {
        HttpHeaderName::from_bytes(src)
            .map(Self)
            .map_err(|_| HeaderError::invalid_name("invalid HTTP header name"))
    }

    /// Returns the lowercase name.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Returns the name length in bytes.
    pub fn len(&self) -> usize {
        self.0.as_str().len()
    }

    /// Header names are never empty.
    pub fn is_empty(&self) -> bool {
        false
    }
}

impl fmt::Debug for HeaderName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_str())
    }
}

impl fmt::Display for HeaderName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AsRef<str> for HeaderName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<str> for HeaderName {
    fn eq(&self, other: &str) -> bool {
        self.as_str().eq_ignore_ascii_case(other)
    }
}

impl PartialEq<&str> for HeaderName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq_ignore_ascii_case(other)
    }
}

/// A borrowed, validated HTTP field name.
pub trait AsHeaderName {
    /// Returns the name as a string slice.
    fn as_header_name(&self) -> &str;
}

impl AsHeaderName for HeaderName {
    fn as_header_name(&self) -> &str {
        self.as_str()
    }
}

impl AsHeaderName for &HeaderName {
    fn as_header_name(&self) -> &str {
        self.as_str()
    }
}

impl AsHeaderName for str {
    fn as_header_name(&self) -> &str {
        self
    }
}

impl AsHeaderName for &str {
    fn as_header_name(&self) -> &str {
        self
    }
}

impl AsHeaderName for String {
    fn as_header_name(&self) -> &str {
        self
    }
}

/// A value that can be converted into an owned [`HeaderName`].
pub trait IntoHeaderName {
    /// Converts the value, validating custom names.
    fn try_into_header_name(self) -> Result<HeaderName, HeaderError>;
}

impl IntoHeaderName for HeaderName {
    fn try_into_header_name(self) -> Result<HeaderName, HeaderError> {
        Ok(self)
    }
}

impl IntoHeaderName for &HeaderName {
    fn try_into_header_name(self) -> Result<HeaderName, HeaderError> {
        Ok(self.clone())
    }
}

impl IntoHeaderName for &str {
    fn try_into_header_name(self) -> Result<HeaderName, HeaderError> {
        HeaderName::from_bytes(self.as_bytes())
    }
}

impl IntoHeaderName for String {
    fn try_into_header_name(self) -> Result<HeaderName, HeaderError> {
        HeaderName::from_bytes(self.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_normalized_and_validated() {
        assert_eq!(
            HeaderName::from_bytes(b"Content-Length").unwrap(),
            "content-length"
        );
        assert!(HeaderName::from_bytes(b"").is_err());
        assert!(HeaderName::from_bytes(b"bad name").is_err());
        assert!(HeaderName::from_bytes(b"bad:name").is_err());
        assert!(HeaderName::from_bytes(b"\xff").is_err());
    }

    #[test]
    fn static_names_are_cheap() {
        let a = HeaderName::from_static("host");
        let b = HeaderName::from_static("host");
        assert_eq!(a, b);
        assert_eq!(a.as_str(), "host");
    }
}
