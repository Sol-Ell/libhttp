use std::fmt;

use http::header::HeaderValue as HttpHeaderValue;

use super::HeaderError;

/// An owned, validated HTTP field value.
///
/// Control bytes (including `CR`, `LF`, `NUL`, `DEL`, and tab) are rejected. Bytes in
/// `0x80..=0xFF` (obs-text) are accepted.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct HeaderValue(HttpHeaderValue);

impl HeaderValue {
    /// Creates a value from a static string.
    ///
    /// # Panics
    ///
    /// Panics at compile time if the string contains a forbidden byte. Prefer
    /// [`HeaderValue::try_from`] for runtime input.
    pub const fn from_static(src: &'static str) -> Self {
        Self(HttpHeaderValue::from_static(src))
    }

    /// Validates and copies a byte slice.
    pub fn from_bytes(src: &[u8]) -> Result<Self, HeaderError> {
        validate_value(src)?;
        HttpHeaderValue::from_bytes(src)
            .map(Self)
            .map_err(|_| HeaderError::invalid_value("invalid HTTP header value"))
    }

    /// Returns the value bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Returns the value as UTF-8, or an error if it is not valid UTF-8.
    pub fn to_str(&self) -> Result<&str, HeaderError> {
        std::str::from_utf8(self.as_bytes())
            .map_err(|_| HeaderError::invalid_value("header value is not valid UTF-8"))
    }

    /// Returns the value length in bytes.
    pub fn len(&self) -> usize {
        self.0.as_bytes().len()
    }

    /// Whether the value is empty.
    pub fn is_empty(&self) -> bool {
        self.0.as_bytes().is_empty()
    }
}

impl TryFrom<&str> for HeaderValue {
    type Error = HeaderError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_bytes(value.as_bytes())
    }
}

impl TryFrom<&[u8]> for HeaderValue {
    type Error = HeaderError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::from_bytes(value)
    }
}

impl TryFrom<String> for HeaderValue {
    type Error = HeaderError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_bytes(value.as_bytes())
    }
}

impl TryFrom<Vec<u8>> for HeaderValue {
    type Error = HeaderError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::from_bytes(&value)
    }
}

impl fmt::Display for HeaderValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&String::from_utf8_lossy(self.as_bytes()))
    }
}

impl fmt::Debug for HeaderValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("HeaderValue")
            .field(&String::from_utf8_lossy(self.as_bytes()))
            .finish()
    }
}

/// A borrowed, validated HTTP field value.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct HeaderRef<'a> {
    bytes: &'a [u8],
}

impl<'a> HeaderRef<'a> {
    /// Validates a borrowed byte slice.
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self, HeaderError> {
        validate_value(bytes)?;
        Ok(Self { bytes })
    }

    pub(crate) fn from_validated(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Returns the value bytes.
    pub fn as_bytes(self) -> &'a [u8] {
        self.bytes
    }

    /// Returns the value as UTF-8, or an error if it is not valid UTF-8.
    pub fn to_str(self) -> Result<&'a str, HeaderError> {
        std::str::from_utf8(self.bytes)
            .map_err(|_| HeaderError::invalid_value("header value is not valid UTF-8"))
    }

    /// Returns the value length in bytes.
    pub fn len(self) -> usize {
        self.bytes.len()
    }

    /// Whether the value is empty.
    pub fn is_empty(self) -> bool {
        self.bytes.is_empty()
    }
}

impl<'a> From<&'a HeaderValue> for HeaderRef<'a> {
    fn from(value: &'a HeaderValue) -> Self {
        Self::from_validated(value.as_bytes())
    }
}

impl<'a> fmt::Display for HeaderRef<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&String::from_utf8_lossy(self.bytes))
    }
}

impl<'a> fmt::Debug for HeaderRef<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("HeaderRef")
            .field(&String::from_utf8_lossy(self.bytes))
            .finish()
    }
}

/// A value that can be converted into an owned [`HeaderValue`].
pub trait IntoHeaderValue {
    /// Converts and validates the value.
    fn try_into_header_value(self) -> Result<HeaderValue, HeaderError>;
}

impl IntoHeaderValue for HeaderValue {
    fn try_into_header_value(self) -> Result<HeaderValue, HeaderError> {
        Ok(self)
    }
}

impl IntoHeaderValue for &HeaderValue {
    fn try_into_header_value(self) -> Result<HeaderValue, HeaderError> {
        Ok(self.clone())
    }
}

impl IntoHeaderValue for &str {
    fn try_into_header_value(self) -> Result<HeaderValue, HeaderError> {
        HeaderValue::try_from(self)
    }
}

impl IntoHeaderValue for String {
    fn try_into_header_value(self) -> Result<HeaderValue, HeaderError> {
        HeaderValue::try_from(self)
    }
}

impl IntoHeaderValue for &[u8] {
    fn try_into_header_value(self) -> Result<HeaderValue, HeaderError> {
        HeaderValue::try_from(self)
    }
}

impl IntoHeaderValue for Vec<u8> {
    fn try_into_header_value(self) -> Result<HeaderValue, HeaderError> {
        HeaderValue::try_from(self)
    }
}

pub(crate) fn validate_value(bytes: &[u8]) -> Result<(), HeaderError> {
    if bytes.iter().any(|byte| is_forbidden_value_byte(*byte)) {
        return Err(HeaderError::invalid_value(
            "header value contains a control character",
        ));
    }
    Ok(())
}

fn is_forbidden_value_byte(byte: u8) -> bool {
    byte < 0x20 || byte == 0x7f
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_control_bytes() {
        assert!(HeaderValue::from_bytes(b"a\r\nb").is_err());
        assert!(HeaderValue::from_bytes(b"a\0b").is_err());
        assert!(HeaderValue::from_bytes(b"a\tb").is_err());
        assert!(HeaderValue::from_bytes(b"a\x7fb").is_err());
    }

    #[test]
    fn allows_visible_and_obs_text() {
        assert_eq!(
            HeaderValue::from_bytes(b"hello world").unwrap().as_bytes(),
            b"hello world"
        );
        assert!(HeaderValue::from_bytes(b"caf\xc3\xa9").is_ok());
        assert!(HeaderValue::from_bytes(&[0x80, 0xff]).is_ok());
        assert!(HeaderValue::from_bytes(b"").is_ok());
    }

    #[test]
    fn ref_and_owned_agree() {
        let value = HeaderValue::try_from("value").unwrap();
        let view = HeaderRef::from(&value);
        assert_eq!(view.as_bytes(), b"value");
        assert_eq!(view.to_str().unwrap(), "value");
    }
}
