use std::borrow::Cow;
use std::fmt;

/// The category of a [`HeaderError`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HeaderErrorKind {
    /// A header name was not a valid lowercase HTTP token.
    InvalidName,
    /// A header value contained a forbidden byte.
    InvalidValue,
    /// A singleton header was repeated.
    Duplicate,
    /// A typed header failed to decode an incoming value.
    Decode,
    /// A typed header failed to encode an outgoing value.
    Encode,
}

/// A typed header decode, encode, validation, or registration error.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct HeaderError {
    kind: HeaderErrorKind,
    message: Cow<'static, str>,
}

impl HeaderError {
    /// Returns the error category.
    pub fn kind(&self) -> HeaderErrorKind {
        self.kind
    }

    /// Returns the human-readable detail.
    pub fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn invalid_name(detail: &'static str) -> Self {
        Self {
            kind: HeaderErrorKind::InvalidName,
            message: Cow::Borrowed(detail),
        }
    }

    pub(crate) fn invalid_value(detail: impl Into<Cow<'static, str>>) -> Self {
        Self {
            kind: HeaderErrorKind::InvalidValue,
            message: detail.into(),
        }
    }

    pub(crate) fn duplicate(name: &str) -> Self {
        Self {
            kind: HeaderErrorKind::Duplicate,
            message: Cow::Owned(format!("header `{name}` must not be repeated")),
        }
    }

    pub(crate) fn decode(detail: impl Into<Cow<'static, str>>) -> Self {
        Self {
            kind: HeaderErrorKind::Decode,
            message: detail.into(),
        }
    }

    pub(crate) fn encode(detail: impl Into<Cow<'static, str>>) -> Self {
        Self {
            kind: HeaderErrorKind::Encode,
            message: detail.into(),
        }
    }
}

impl fmt::Display for HeaderErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::InvalidName => "invalid name",
            Self::InvalidValue => "invalid value",
            Self::Duplicate => "duplicate",
            Self::Decode => "decode",
            Self::Encode => "encode",
        };
        f.write_str(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn carries_kind_and_message() {
        let error = HeaderError::duplicate("content-length");
        assert_eq!(error.kind(), HeaderErrorKind::Duplicate);
        assert!(error.message().contains("content-length"));
        assert_eq!(error.to_string(), error.message());
    }
}
