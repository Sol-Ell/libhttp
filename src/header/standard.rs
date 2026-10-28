use std::time::SystemTime;

use super::{Header, HeaderError, HeaderName, HeaderRef, HeaderValue};

macro_rules! name {
    ($const:ident, $wire:literal) => {
        pub const $const: HeaderName = HeaderName::from_static($wire);
    };
}

name!(HOST, "host");
name!(CONTENT_LENGTH, "content-length");
name!(CONTENT_TYPE, "content-type");
name!(CONTENT_ENCODING, "content-encoding");
name!(CONTENT_LANGUAGE, "content-language");
name!(CONTENT_LOCATION, "content-location");
name!(CONTENT_RANGE, "content-range");
name!(DATE, "date");
name!(AGE, "age");
name!(EXPIRES, "expires");
name!(CACHE_CONTROL, "cache-control");
name!(PRAGMA, "pragma");
name!(VARY, "vary");
name!(ETAG, "etag");
name!(LAST_MODIFIED, "last-modified");
name!(IF_MATCH, "if-match");
name!(IF_NONE_MATCH, "if-none-match");
name!(IF_MODIFIED_SINCE, "if-modified-since");
name!(IF_UNMODIFIED_SINCE, "if-unmodified-since");
name!(IF_RANGE, "if-range");
name!(RANGE, "range");
name!(ACCEPT_RANGES, "accept-ranges");
name!(CONNECTION, "connection");
name!(TRANSFER_ENCODING, "transfer-encoding");
name!(TE, "te");
name!(TRAILER, "trailer");
name!(EXPECT, "expect");
name!(COOKIE, "cookie");
name!(SET_COOKIE, "set-cookie");
name!(ACCEPT, "accept");
name!(ACCEPT_ENCODING, "accept-encoding");
name!(ACCEPT_LANGUAGE, "accept-language");
name!(AUTHORIZATION, "authorization");
name!(PROXY_AUTHORIZATION, "proxy-authorization");
name!(WWW_AUTHENTICATE, "www-authenticate");
name!(PROXY_AUTHENTICATE, "proxy-authenticate");
name!(LOCATION, "location");
name!(REFERER, "referer");
name!(USER_AGENT, "user-agent");
name!(SERVER, "server");
name!(RETRY_AFTER, "retry-after");
name!(ALLOW, "allow");
name!(LINK, "link");
name!(WARNING, "warning");

fn text_value(value: HeaderRef<'_>, field: &str) -> Result<String, HeaderError> {
    let text = value
        .to_str()
        .map_err(|_| HeaderError::decode(format!("{field} is not valid UTF-8")))?;
    let text = text.trim_matches(' ');
    if text.is_empty() {
        return Err(HeaderError::decode(format!("{field} must not be empty")));
    }
    Ok(text.to_owned())
}

macro_rules! text_header {
    ($ty:ident, $const:ident, $multiple:expr) => {
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $ty(String);

        impl $ty {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Header for $ty {
            fn name() -> HeaderName {
                $const
            }

            const MULTIPLE: bool = $multiple;

            fn decode(value: HeaderRef<'_>) -> Result<Self, HeaderError> {
                Ok(Self(text_value(value, $const.as_str())?))
            }

            fn encode(&self) -> Result<HeaderValue, HeaderError> {
                HeaderValue::try_from(self.0.as_str())
            }
        }
    };
}

/// The `Host` request field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Host(String);

impl Host {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The host portion, without brackets or port where it can be determined.
    pub fn hostname(&self) -> &str {
        let value = self.0.as_str();
        if let Some(rest) = value.strip_prefix('[') {
            if let Some(end) = rest.find(']') {
                return &rest[..end];
            }
        }
        match value.rsplit_once(':') {
            Some((host, port)) if !host.is_empty() && port.parse::<u16>().is_ok() => host,
            _ => value,
        }
    }

    /// The port, if present and numeric.
    pub fn port(&self) -> Option<u16> {
        let value = self.0.as_str();
        if value.starts_with('[') {
            let end = value.find(']')?;
            let rest = value.get(end + 1..)?;
            return rest.strip_prefix(':')?.parse().ok();
        }
        let (_, port) = value.rsplit_once(':')?;
        port.parse().ok()
    }
}

impl Header for Host {
    fn name() -> HeaderName {
        HOST
    }

    fn decode(value: HeaderRef<'_>) -> Result<Self, HeaderError> {
        let text = text_value(value, "host")?;
        validate_host(&text)?;
        Ok(Self(text))
    }

    fn encode(&self) -> Result<HeaderValue, HeaderError> {
        validate_host(&self.0)?;
        HeaderValue::try_from(self.0.as_str())
    }
}

fn validate_host(value: &str) -> Result<(), HeaderError> {
    if value.is_empty()
        || value
            .bytes()
            .any(|byte| byte <= b' ' || matches!(byte, b'/' | b'?' | b'#' | b'@' | b',' | b'\\'))
    {
        return Err(HeaderError::decode("invalid Host value"));
    }
    Ok(())
}

/// The `Content-Length` field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContentLength(u64);

impl ContentLength {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl Header for ContentLength {
    fn name() -> HeaderName {
        CONTENT_LENGTH
    }

    fn decode(value: HeaderRef<'_>) -> Result<Self, HeaderError> {
        let text = text_value(value, "content-length")?;
        if !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(HeaderError::decode("content-length must be decimal digits"));
        }
        let value = text
            .parse::<u64>()
            .map_err(|_| HeaderError::decode("content-length is out of range"))?;
        Ok(Self(value))
    }

    fn encode(&self) -> Result<HeaderValue, HeaderError> {
        HeaderValue::try_from(self.0.to_string())
    }
}

/// The `Content-Type` field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContentType(String);

impl ContentType {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn json() -> Self {
        Self::new("application/json")
    }

    pub fn text_plain() -> Self {
        Self::new("text/plain; charset=utf-8")
    }

    pub fn text_html() -> Self {
        Self::new("text/html; charset=utf-8")
    }

    pub fn octet_stream() -> Self {
        Self::new("application/octet-stream")
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The media type portion before any parameters.
    pub fn media_type(&self) -> &str {
        self.0.split(';').next().unwrap_or("").trim()
    }
}

impl Header for ContentType {
    fn name() -> HeaderName {
        CONTENT_TYPE
    }

    fn decode(value: HeaderRef<'_>) -> Result<Self, HeaderError> {
        Ok(Self(text_value(value, "content-type")?))
    }

    fn encode(&self) -> Result<HeaderValue, HeaderError> {
        HeaderValue::try_from(self.0.as_str())
    }
}

/// The `Date`, `Expires`, `Last-Modified`, `If-Modified-Since` and `If-Unmodified-Since` value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Date(SystemTime);

impl Date {
    pub fn now() -> Self {
        Self(SystemTime::now())
    }

    pub const fn from_system_time(time: SystemTime) -> Self {
        Self(time)
    }

    pub const fn to_system_time(self) -> SystemTime {
        self.0
    }

    pub fn as_http_date(self) -> String {
        httpdate::fmt_http_date(self.0)
    }
}

impl Header for Date {
    fn name() -> HeaderName {
        DATE
    }

    fn decode(value: HeaderRef<'_>) -> Result<Self, HeaderError> {
        let text = text_value(value, "date")?;
        httpdate::parse_http_date(&text)
            .map(Self)
            .map_err(|_| HeaderError::decode("invalid HTTP date"))
    }

    fn encode(&self) -> Result<HeaderValue, HeaderError> {
        HeaderValue::try_from(httpdate::fmt_http_date(self.0))
    }
}

macro_rules! date_header {
    ($ty:ident, $const:ident) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub struct $ty(Date);

        impl $ty {
            pub const fn new(date: Date) -> Self {
                Self(date)
            }

            pub const fn date(self) -> Date {
                self.0
            }
        }

        impl Header for $ty {
            fn name() -> HeaderName {
                $const
            }

            fn decode(value: HeaderRef<'_>) -> Result<Self, HeaderError> {
                <Date as Header>::decode(value).map(Self)
            }

            fn encode(&self) -> Result<HeaderValue, HeaderError> {
                self.0.encode()
            }
        }
    };
}

date_header!(Expires, EXPIRES);
date_header!(LastModified, LAST_MODIFIED);
date_header!(IfModifiedSince, IF_MODIFIED_SINCE);
date_header!(IfUnmodifiedSince, IF_UNMODIFIED_SINCE);

/// The `Age` field in seconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Age(u64);

impl Age {
    pub const fn new(seconds: u64) -> Self {
        Self(seconds)
    }

    pub const fn seconds(self) -> u64 {
        self.0
    }
}

impl Header for Age {
    fn name() -> HeaderName {
        AGE
    }

    fn decode(value: HeaderRef<'_>) -> Result<Self, HeaderError> {
        let text = text_value(value, "age")?;
        if !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(HeaderError::decode("age must be decimal digits"));
        }
        Ok(Self(
            text.parse()
                .map_err(|_| HeaderError::decode("age is out of range"))?,
        ))
    }

    fn encode(&self) -> Result<HeaderValue, HeaderError> {
        HeaderValue::try_from(self.0.to_string())
    }
}

/// The `Retry-After` field: delay seconds or an HTTP date.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryAfter {
    Seconds(u64),
    Date(Date),
}

impl RetryAfter {
    pub const fn seconds(seconds: u64) -> Self {
        Self::Seconds(seconds)
    }

    pub const fn date(date: Date) -> Self {
        Self::Date(date)
    }
}

impl Header for RetryAfter {
    fn name() -> HeaderName {
        RETRY_AFTER
    }

    fn decode(value: HeaderRef<'_>) -> Result<Self, HeaderError> {
        let text = text_value(value, "retry-after")?;
        if text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Ok(Self::Seconds(text.parse().map_err(|_| {
                HeaderError::decode("retry-after is out of range")
            })?));
        }
        httpdate::parse_http_date(&text)
            .map(|date| Self::Date(Date::from_system_time(date)))
            .map_err(|_| HeaderError::decode("invalid retry-after value"))
    }

    fn encode(&self) -> Result<HeaderValue, HeaderError> {
        match self {
            Self::Seconds(seconds) => HeaderValue::try_from(seconds.to_string()),
            Self::Date(date) => date.encode(),
        }
    }
}

text_header!(ContentEncoding, CONTENT_ENCODING, true);
text_header!(ContentLanguage, CONTENT_LANGUAGE, true);
text_header!(ContentLocation, CONTENT_LOCATION, false);
text_header!(ContentRange, CONTENT_RANGE, false);
text_header!(CacheControl, CACHE_CONTROL, true);
text_header!(Pragma, PRAGMA, true);
text_header!(Vary, VARY, true);
text_header!(ETag, ETAG, false);
text_header!(IfMatch, IF_MATCH, true);
text_header!(IfNoneMatch, IF_NONE_MATCH, true);
text_header!(IfRange, IF_RANGE, false);
text_header!(Range, RANGE, false);
text_header!(AcceptRanges, ACCEPT_RANGES, false);
text_header!(Connection, CONNECTION, true);
text_header!(TransferEncoding, TRANSFER_ENCODING, true);
text_header!(Te, TE, true);
text_header!(Trailer, TRAILER, true);
text_header!(Expect, EXPECT, false);
text_header!(Cookie, COOKIE, true);
text_header!(SetCookie, SET_COOKIE, true);
text_header!(Accept, ACCEPT, true);
text_header!(AcceptEncoding, ACCEPT_ENCODING, true);
text_header!(AcceptLanguage, ACCEPT_LANGUAGE, true);
text_header!(Authorization, AUTHORIZATION, false);
text_header!(ProxyAuthorization, PROXY_AUTHORIZATION, false);
text_header!(WwwAuthenticate, WWW_AUTHENTICATE, true);
text_header!(ProxyAuthenticate, PROXY_AUTHENTICATE, true);
text_header!(Location, LOCATION, false);
text_header!(Referer, REFERER, false);
text_header!(UserAgent, USER_AGENT, false);
text_header!(Server, SERVER, false);
text_header!(Allow, ALLOW, true);
text_header!(Link, LINK, true);
text_header!(Warning, WARNING, true);

impl SetCookie {
    pub fn from_pair(name: &str, value: &str) -> Self {
        Self(format!("{name}={value}"))
    }
}

impl Connection {
    pub fn close() -> Self {
        Self::new("close")
    }

    pub fn keep_alive() -> Self {
        Self::new("keep-alive")
    }
}

impl TransferEncoding {
    pub fn chunked() -> Self {
        Self::new("chunked")
    }
}

impl Expect {
    pub fn continue_100() -> Self {
        Self::new("100-continue")
    }
}

impl Te {
    pub fn trailers() -> Self {
        Self::new("trailers")
    }
}

impl Accept {
    pub fn any() -> Self {
        Self::new("*/*")
    }

    pub fn json() -> Self {
        Self::new("application/json")
    }
}

impl AcceptEncoding {
    pub fn gzip() -> Self {
        Self::new("gzip")
    }
}

impl CacheControl {
    pub fn no_store() -> Self {
        Self::new("no-store")
    }
}

impl Vary {
    pub fn accept_encoding() -> Self {
        Self::new("accept-encoding")
    }
}

impl Authorization {
    pub fn bearer(token: &str) -> Self {
        Self(format!("Bearer {token}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_parses_hostname_and_port() {
        let host = Host::new("example.com:8080");
        assert_eq!(host.hostname(), "example.com");
        assert_eq!(host.port(), Some(8080));

        let host = Host::new("[::1]:443");
        assert_eq!(host.hostname(), "::1");
        assert_eq!(host.port(), Some(443));
    }

    #[test]
    fn content_length_is_numeric() {
        let value = ContentLength::decode(HeaderRef::from_bytes(b"123").unwrap()).unwrap();
        assert_eq!(value.get(), 123);
        assert!(ContentLength::decode(HeaderRef::from_bytes(b"12x").unwrap()).is_err());
    }

    #[test]
    fn date_round_trips() {
        let date = Date::from_system_time(SystemTime::UNIX_EPOCH);
        let encoded = date.encode().unwrap();
        let decoded = Date::decode(HeaderRef::from(&encoded)).unwrap();
        assert_eq!(decoded, date);
    }

    #[test]
    fn header_constants_are_normalized() {
        assert_eq!(HOST.as_str(), "host");
        assert_eq!(CONTENT_LENGTH.as_str(), "content-length");
        assert_eq!(WWW_AUTHENTICATE.as_str(), "www-authenticate");
    }

    #[test]
    fn setters_construct_wire_values() {
        assert_eq!(ContentType::json().as_str(), "application/json");
        assert_eq!(SetCookie::from_pair("a", "b").as_str(), "a=b");
        assert_eq!(Expect::continue_100().as_str(), "100-continue");
    }
}
