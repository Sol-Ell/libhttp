use std::any::{Any, TypeId};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

const INLINE_CAPACITY: usize = 32;

/// A validated, lowercase HTTP field name.
#[derive(Clone)]
pub struct HeaderName {
    repr: NameRepr,
}

#[derive(Clone)]
enum NameRepr {
    Static(&'static str),
    Inline { len: u8, buf: [u8; INLINE_CAPACITY] },
    Boxed(Box<str>),
}

impl HeaderName {
    pub const ACCEPT: HeaderName = HeaderName::const_name("accept");
    pub const ACCEPT_ENCODING: HeaderName = HeaderName::const_name("accept-encoding");
    pub const ACCEPT_LANGUAGE: HeaderName = HeaderName::const_name("accept-language");
    pub const ACCEPT_RANGES: HeaderName = HeaderName::const_name("accept-ranges");
    pub const AGE: HeaderName = HeaderName::const_name("age");
    pub const ALLOW: HeaderName = HeaderName::const_name("allow");
    pub const AUTHORIZATION: HeaderName = HeaderName::const_name("authorization");
    pub const CACHE_CONTROL: HeaderName = HeaderName::const_name("cache-control");
    pub const CONNECTION: HeaderName = HeaderName::const_name("connection");
    pub const CONTENT_ENCODING: HeaderName = HeaderName::const_name("content-encoding");
    pub const CONTENT_LANGUAGE: HeaderName = HeaderName::const_name("content-language");
    pub const CONTENT_LENGTH: HeaderName = HeaderName::const_name("content-length");
    pub const CONTENT_LOCATION: HeaderName = HeaderName::const_name("content-location");
    pub const CONTENT_RANGE: HeaderName = HeaderName::const_name("content-range");
    pub const CONTENT_TYPE: HeaderName = HeaderName::const_name("content-type");
    pub const COOKIE: HeaderName = HeaderName::const_name("cookie");
    pub const DATE: HeaderName = HeaderName::const_name("date");
    pub const ETAG: HeaderName = HeaderName::const_name("etag");
    pub const EXPECT: HeaderName = HeaderName::const_name("expect");
    pub const EXPIRES: HeaderName = HeaderName::const_name("expires");
    pub const HOST: HeaderName = HeaderName::const_name("host");
    pub const IF_MATCH: HeaderName = HeaderName::const_name("if-match");
    pub const IF_MODIFIED_SINCE: HeaderName = HeaderName::const_name("if-modified-since");
    pub const IF_NONE_MATCH: HeaderName = HeaderName::const_name("if-none-match");
    pub const IF_RANGE: HeaderName = HeaderName::const_name("if-range");
    pub const IF_UNMODIFIED_SINCE: HeaderName = HeaderName::const_name("if-unmodified-since");
    pub const LAST_MODIFIED: HeaderName = HeaderName::const_name("last-modified");
    pub const LINK: HeaderName = HeaderName::const_name("link");
    pub const LOCATION: HeaderName = HeaderName::const_name("location");
    pub const PRAGMA: HeaderName = HeaderName::const_name("pragma");
    pub const PROXY_AUTHENTICATE: HeaderName = HeaderName::const_name("proxy-authenticate");
    pub const PROXY_AUTHORIZATION: HeaderName = HeaderName::const_name("proxy-authorization");
    pub const RANGE: HeaderName = HeaderName::const_name("range");
    pub const REFERER: HeaderName = HeaderName::const_name("referer");
    pub const RETRY_AFTER: HeaderName = HeaderName::const_name("retry-after");
    pub const SERVER: HeaderName = HeaderName::const_name("server");
    pub const SET_COOKIE: HeaderName = HeaderName::const_name("set-cookie");
    pub const TE: HeaderName = HeaderName::const_name("te");
    pub const TRAILER: HeaderName = HeaderName::const_name("trailer");
    pub const TRANSFER_ENCODING: HeaderName = HeaderName::const_name("transfer-encoding");
    pub const USER_AGENT: HeaderName = HeaderName::const_name("user-agent");
    pub const VARY: HeaderName = HeaderName::const_name("vary");
    pub const WARNING: HeaderName = HeaderName::const_name("warning");
    pub const WWW_AUTHENTICATE: HeaderName = HeaderName::const_name("www-authenticate");

    const fn const_name(value: &'static str) -> Self {
        HeaderName {
            repr: NameRepr::Static(value),
        }
    }

    /// Validate a field name and normalize it to lowercase.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, HeaderError> {
        if !is_name_token(bytes) {
            return Err(HeaderError::invalid_name());
        }
        if bytes.len() <= INLINE_CAPACITY {
            let mut buf = [0u8; INLINE_CAPACITY];
            for (slot, &byte) in buf.iter_mut().zip(bytes) {
                *slot = byte.to_ascii_lowercase();
            }
            Ok(HeaderName {
                repr: NameRepr::Inline {
                    len: bytes.len() as u8,
                    buf,
                },
            })
        } else {
            let lowered = bytes
                .iter()
                .map(|byte| byte.to_ascii_lowercase())
                .collect::<Vec<_>>();
            let text = String::from_utf8(lowered).map_err(|_| HeaderError::invalid_name())?;
            Ok(HeaderName {
                repr: NameRepr::Boxed(text.into_boxed_str()),
            })
        }
    }

    /// Validate a static field name. An already-lowercase name is stored
    /// without allocating.
    pub fn from_static(value: &'static str) -> Result<Self, HeaderError> {
        if !is_name_token(value.as_bytes()) {
            return Err(HeaderError::invalid_name());
        }
        if value.bytes().all(|byte| !byte.is_ascii_uppercase()) {
            Ok(HeaderName {
                repr: NameRepr::Static(value),
            })
        } else {
            HeaderName::from_bytes(value.as_bytes())
        }
    }

    pub fn as_str(&self) -> &str {
        match &self.repr {
            NameRepr::Static(value) => value,
            NameRepr::Inline { len, buf } => std::str::from_utf8(&buf[..*len as usize])
                .expect("validated header names are ASCII"),
            NameRepr::Boxed(value) => value,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}

impl fmt::Display for HeaderName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for HeaderName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("HeaderName").field(&self.as_str()).finish()
    }
}

impl PartialEq for HeaderName {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for HeaderName {}

impl PartialOrd for HeaderName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeaderName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl Hash for HeaderName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl AsRef<str> for HeaderName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for HeaderName {
    type Err = HeaderError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        HeaderName::from_bytes(value.as_bytes())
    }
}

impl TryFrom<&str> for HeaderName {
    type Error = HeaderError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        HeaderName::from_bytes(value.as_bytes())
    }
}

impl TryFrom<String> for HeaderName {
    type Error = HeaderError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        HeaderName::from_bytes(value.as_bytes())
    }
}

impl TryFrom<&[u8]> for HeaderName {
    type Error = HeaderError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        HeaderName::from_bytes(value)
    }
}

/// A validated, owned HTTP field value.
#[derive(Clone)]
pub struct HeaderValue {
    repr: ValueRepr,
}

#[derive(Clone)]
enum ValueRepr {
    Static(&'static [u8]),
    Inline { len: u8, buf: [u8; INLINE_CAPACITY] },
    Boxed(Box<[u8]>),
}

impl HeaderValue {
    /// Validate a field value from bytes. CR, LF, NUL and all other controls
    /// are rejected; obs-text (0x80..=0xFF) is permitted.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, HeaderError> {
        if !is_field_value(bytes) {
            return Err(HeaderError::invalid_value());
        }
        if bytes.len() <= INLINE_CAPACITY {
            let mut buf = [0u8; INLINE_CAPACITY];
            buf[..bytes.len()].copy_from_slice(bytes);
            Ok(HeaderValue {
                repr: ValueRepr::Inline {
                    len: bytes.len() as u8,
                    buf,
                },
            })
        } else {
            Ok(HeaderValue {
                repr: ValueRepr::Boxed(bytes.to_vec().into_boxed_slice()),
            })
        }
    }

    /// Validate a static string value, storing it without allocating.
    pub fn from_static(value: &'static str) -> Result<Self, HeaderError> {
        HeaderValue::from_static_bytes(value.as_bytes())
    }

    /// Validate a static byte value, storing it without allocating.
    pub fn from_static_bytes(value: &'static [u8]) -> Result<Self, HeaderError> {
        if !is_field_value(value) {
            return Err(HeaderError::invalid_value());
        }
        Ok(HeaderValue {
            repr: ValueRepr::Static(value),
        })
    }

    pub fn empty() -> Self {
        HeaderValue {
            repr: ValueRepr::Static(b""),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        match &self.repr {
            ValueRepr::Static(value) => value,
            ValueRepr::Inline { len, buf } => &buf[..*len as usize],
            ValueRepr::Boxed(value) => value,
        }
    }

    pub fn to_str(&self) -> Option<&str> {
        std::str::from_utf8(self.as_bytes()).ok()
    }

    pub fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }

    pub fn len(&self) -> usize {
        self.as_bytes().len()
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

impl PartialEq for HeaderValue {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for HeaderValue {}

impl Hash for HeaderValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl AsRef<[u8]> for HeaderValue {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl FromStr for HeaderValue {
    type Err = HeaderError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        HeaderValue::from_bytes(value.as_bytes())
    }
}

impl TryFrom<&str> for HeaderValue {
    type Error = HeaderError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        HeaderValue::from_bytes(value.as_bytes())
    }
}

impl TryFrom<String> for HeaderValue {
    type Error = HeaderError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        HeaderValue::from_bytes(value.as_bytes())
    }
}

impl TryFrom<&[u8]> for HeaderValue {
    type Error = HeaderError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        HeaderValue::from_bytes(value)
    }
}

impl TryFrom<Vec<u8>> for HeaderValue {
    type Error = HeaderError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        HeaderValue::from_bytes(&value)
    }
}

/// A borrowed, validated view over one field-line value.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct HeaderRef<'a> {
    bytes: &'a [u8],
}

impl<'a> HeaderRef<'a> {
    /// Validate a borrowed field value.
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self, HeaderError> {
        if is_field_value(bytes) {
            Ok(HeaderRef { bytes })
        } else {
            Err(HeaderError::invalid_value())
        }
    }

    pub(crate) fn from_validated(bytes: &'a [u8]) -> Self {
        HeaderRef { bytes }
    }

    pub fn as_bytes(self) -> &'a [u8] {
        self.bytes
    }

    pub fn to_str(self) -> Option<&'a str> {
        std::str::from_utf8(self.bytes).ok()
    }

    pub fn is_empty(self) -> bool {
        self.bytes.is_empty()
    }

    pub fn len(self) -> usize {
        self.bytes.len()
    }
}

impl fmt::Display for HeaderRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&String::from_utf8_lossy(self.bytes))
    }
}

impl fmt::Debug for HeaderRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("HeaderRef")
            .field(&String::from_utf8_lossy(self.bytes))
            .finish()
    }
}

impl<'a> From<&'a HeaderValue> for HeaderRef<'a> {
    fn from(value: &'a HeaderValue) -> Self {
        HeaderRef::from_validated(value.as_bytes())
    }
}

impl<'a> TryFrom<&'a [u8]> for HeaderRef<'a> {
    type Error = HeaderError;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        HeaderRef::from_bytes(value)
    }
}

impl<'a> TryFrom<&'a str> for HeaderRef<'a> {
    type Error = HeaderError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        HeaderRef::from_bytes(value.as_bytes())
    }
}

/// A header name/value validation, decode, encode or registration failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeaderError {
    message: &'static str,
}

impl HeaderError {
    pub fn invalid_name() -> Self {
        HeaderError {
            message: "invalid header name",
        }
    }

    pub fn invalid_value() -> Self {
        HeaderError {
            message: "invalid header value",
        }
    }

    pub fn decode(message: &'static str) -> Self {
        HeaderError { message }
    }

    pub fn encode(message: &'static str) -> Self {
        HeaderError { message }
    }

    pub fn registration(message: &'static str) -> Self {
        HeaderError { message }
    }

    pub(crate) fn duplicate() -> Self {
        HeaderError {
            message: "duplicate singleton header",
        }
    }

    pub fn message(&self) -> &'static str {
        self.message
    }
}

impl fmt::Display for HeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message)
    }
}

impl std::error::Error for HeaderError {}

/// A typed header: one implementation per field name.
pub trait Header: Sized + Send + Sync + 'static {
    fn name() -> HeaderName;

    /// Whether this field may appear more than once.
    const MULTIPLE: bool = false;

    fn decode(value: HeaderRef<'_>) -> Result<Self, HeaderError>;

    fn encode(&self) -> Result<HeaderValue, HeaderError>;
}

/// A name accepted by raw lookup APIs.
pub trait AsHeaderName {
    fn as_header_name(&self) -> Option<HeaderName>;
}

impl AsHeaderName for HeaderName {
    fn as_header_name(&self) -> Option<HeaderName> {
        Some(self.clone())
    }
}

impl AsHeaderName for &HeaderName {
    fn as_header_name(&self) -> Option<HeaderName> {
        Some((*self).clone())
    }
}

impl AsHeaderName for &str {
    fn as_header_name(&self) -> Option<HeaderName> {
        HeaderName::from_bytes(self.as_bytes()).ok()
    }
}

impl AsHeaderName for String {
    fn as_header_name(&self) -> Option<HeaderName> {
        HeaderName::from_bytes(self.as_bytes()).ok()
    }
}

/// A name accepted by raw mutation APIs.
pub trait IntoHeaderName {
    fn into_header_name(self) -> Result<HeaderName, HeaderError>;
}

impl IntoHeaderName for HeaderName {
    fn into_header_name(self) -> Result<HeaderName, HeaderError> {
        Ok(self)
    }
}

impl IntoHeaderName for &HeaderName {
    fn into_header_name(self) -> Result<HeaderName, HeaderError> {
        Ok(self.clone())
    }
}

impl IntoHeaderName for &str {
    fn into_header_name(self) -> Result<HeaderName, HeaderError> {
        HeaderName::from_bytes(self.as_bytes())
    }
}

impl IntoHeaderName for String {
    fn into_header_name(self) -> Result<HeaderName, HeaderError> {
        HeaderName::from_bytes(self.as_bytes())
    }
}

impl IntoHeaderName for &[u8] {
    fn into_header_name(self) -> Result<HeaderName, HeaderError> {
        HeaderName::from_bytes(self)
    }
}

impl IntoHeaderName for Vec<u8> {
    fn into_header_name(self) -> Result<HeaderName, HeaderError> {
        HeaderName::from_bytes(&self)
    }
}

/// A value accepted by raw mutation APIs.
pub trait IntoHeaderValue {
    fn into_header_value(self) -> Result<HeaderValue, HeaderError>;
}

impl IntoHeaderValue for HeaderValue {
    fn into_header_value(self) -> Result<HeaderValue, HeaderError> {
        Ok(self)
    }
}

impl IntoHeaderValue for &HeaderValue {
    fn into_header_value(self) -> Result<HeaderValue, HeaderError> {
        Ok(self.clone())
    }
}

impl IntoHeaderValue for &str {
    fn into_header_value(self) -> Result<HeaderValue, HeaderError> {
        HeaderValue::from_bytes(self.as_bytes())
    }
}

impl IntoHeaderValue for String {
    fn into_header_value(self) -> Result<HeaderValue, HeaderError> {
        HeaderValue::from_bytes(self.as_bytes())
    }
}

impl IntoHeaderValue for &[u8] {
    fn into_header_value(self) -> Result<HeaderValue, HeaderError> {
        HeaderValue::from_bytes(self)
    }
}

impl IntoHeaderValue for Vec<u8> {
    fn into_header_value(self) -> Result<HeaderValue, HeaderError> {
        HeaderValue::from_bytes(&self)
    }
}

/// An ordered, duplicate-preserving collection of typed and raw fields.
#[derive(Default)]
pub struct HeaderMap {
    entries: Vec<Entry>,
}

enum Entry {
    Raw {
        name: HeaderName,
        value: HeaderValue,
    },
    Typed {
        name: HeaderName,
        type_id: TypeId,
        value: Box<dyn Any + Send + Sync>,
    },
}

impl HeaderMap {
    pub fn new() -> Self {
        HeaderMap::default()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn get<T: Header>(&self) -> Option<&T> {
        self.entries.iter().find_map(|entry| match entry {
            Entry::Typed { type_id, value, .. } if *type_id == TypeId::of::<T>() => {
                value.downcast_ref::<T>()
            }
            _ => None,
        })
    }

    pub fn get_all<T: Header>(&self) -> impl Iterator<Item = &T> + '_ {
        self.entries.iter().filter_map(|entry| match entry {
            Entry::Typed { type_id, value, .. } if *type_id == TypeId::of::<T>() => {
                value.downcast_ref::<T>()
            }
            _ => None,
        })
    }

    /// Replace every value of `T` with `value`.
    pub fn set<T: Header>(&mut self, value: T) {
        let name = T::name();
        self.remove::<T>();
        self.entries.push(Entry::Typed {
            name,
            type_id: TypeId::of::<T>(),
            value: Box::new(value),
        });
    }

    /// Append a value of `T`, rejecting a duplicate singleton.
    pub fn append<T: Header>(&mut self, value: T) -> Result<(), HeaderError> {
        if !T::MULTIPLE && self.contains_typed::<T>() {
            return Err(HeaderError::duplicate());
        }
        self.entries.push(Entry::Typed {
            name: T::name(),
            type_id: TypeId::of::<T>(),
            value: Box::new(value),
        });
        Ok(())
    }

    pub fn remove<T: Header>(&mut self) {
        let type_id = TypeId::of::<T>();
        self.entries.retain(
            |entry| !matches!(entry, Entry::Typed { type_id: entry_type, .. } if *entry_type == type_id),
        );
    }

    pub fn get_raw(&self, name: impl AsHeaderName) -> Option<HeaderRef<'_>> {
        let name = name.as_header_name()?;
        self.entries.iter().find_map(|entry| match entry {
            Entry::Raw {
                name: entry_name,
                value,
            } if *entry_name == name => Some(HeaderRef::from_validated(value.as_bytes())),
            _ => None,
        })
    }

    pub fn contains_raw(&self, name: impl AsHeaderName) -> bool {
        self.get_raw(name).is_some()
    }

    pub fn append_raw(
        &mut self,
        name: impl IntoHeaderName,
        value: impl IntoHeaderValue,
    ) -> Result<(), HeaderError> {
        let name = name.into_header_name()?;
        let value = value.into_header_value()?;
        self.entries.push(Entry::Raw { name, value });
        Ok(())
    }

    /// Iterate raw entries in insertion order. Typed entries are not encoded
    /// on demand and therefore do not appear here.
    pub fn iter_raw(&self) -> impl Iterator<Item = (&HeaderName, HeaderRef<'_>)> + '_ {
        self.entries.iter().filter_map(|entry| match entry {
            Entry::Raw { name, value } => Some((name, HeaderRef::from_validated(value.as_bytes()))),
            Entry::Typed { .. } => None,
        })
    }

    fn contains_typed<T: Header>(&self) -> bool {
        let type_id = TypeId::of::<T>();
        self.entries
            .iter()
            .any(|entry| matches!(entry, Entry::Typed { type_id: entry_type, .. } if *entry_type == type_id))
    }
}

impl fmt::Debug for HeaderMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        for (name, value) in self.iter_raw() {
            map.entry(&name.as_str(), &value.to_str().unwrap_or("<binary>"));
        }
        for entry in &self.entries {
            if let Entry::Typed { name, .. } = entry {
                map.entry(&name.as_str(), &"<typed>");
            }
        }
        map.finish()
    }
}

fn is_name_token(bytes: &[u8]) -> bool {
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

/// Field values reject NUL, CR, LF, DEL and every other control byte.
fn is_field_value(bytes: &[u8]) -> bool {
    bytes.iter().all(|&byte| byte >= 0x20 && byte != 0x7f)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    struct Host(String);

    impl Header for Host {
        fn name() -> HeaderName {
            HeaderName::HOST
        }

        fn decode(value: HeaderRef<'_>) -> Result<Self, HeaderError> {
            value
                .to_str()
                .map(|value| Host(value.to_owned()))
                .ok_or_else(HeaderError::invalid_value)
        }

        fn encode(&self) -> Result<HeaderValue, HeaderError> {
            HeaderValue::try_from(self.0.as_str())
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct SetCookie(String);

    impl Header for SetCookie {
        const MULTIPLE: bool = true;

        fn name() -> HeaderName {
            HeaderName::SET_COOKIE
        }

        fn decode(value: HeaderRef<'_>) -> Result<Self, HeaderError> {
            value
                .to_str()
                .map(|value| SetCookie(value.to_owned()))
                .ok_or_else(HeaderError::invalid_value)
        }

        fn encode(&self) -> Result<HeaderValue, HeaderError> {
            HeaderValue::try_from(self.0.as_str())
        }
    }

    #[test]
    fn header_name_lowercases_and_rejects_invalid_tokens() {
        assert_eq!(
            HeaderName::from_bytes(b"Content-Type").unwrap().as_str(),
            "content-type"
        );
        assert_eq!(HeaderName::from_bytes(b"HOST").unwrap(), HeaderName::HOST);
        assert_eq!(
            HeaderName::from_static("X-Request-Id").unwrap().as_str(),
            "x-request-id"
        );
        assert!(HeaderName::from_bytes(b"").is_err());
        assert!(HeaderName::from_bytes(b"bad name").is_err());
        assert!(HeaderName::from_bytes(b"bad\nname").is_err());
        assert!(HeaderName::from_bytes(b"bad(name)").is_err());

        let long = format!("x-{}", "a".repeat(40));
        assert_eq!(
            HeaderName::from_bytes(long.as_bytes()).unwrap().as_str(),
            long
        );
    }

    #[test]
    fn header_value_rejects_crlf_and_controls() {
        assert!(HeaderValue::try_from("a\r\nb").is_err());
        assert!(HeaderValue::try_from("a\rb").is_err());
        assert!(HeaderValue::try_from("a\nb").is_err());
        assert!(HeaderValue::try_from("a\0b").is_err());
        assert!(HeaderValue::try_from("a\tb").is_err());
        assert!(HeaderValue::try_from("a\u{7f}b").is_err());
        assert_eq!(
            HeaderValue::try_from("text/plain; charset=utf-8")
                .unwrap()
                .as_bytes(),
            b"text/plain; charset=utf-8"
        );
        assert_eq!(HeaderValue::try_from("").unwrap().as_bytes(), b"");
        // obs-text is permitted in values (but never in names).
        assert_eq!(
            HeaderValue::try_from("caf\u{e9}").unwrap().to_str(),
            Some("caf\u{e9}")
        );

        let long = "v".repeat(64);
        assert_eq!(
            HeaderValue::from_bytes(long.as_bytes()).unwrap().as_bytes(),
            long.as_bytes()
        );
    }

    #[test]
    fn header_ref_validates_and_borrows() {
        let value = HeaderValue::try_from("one").unwrap();
        let header_ref = HeaderRef::from(&value);
        assert_eq!(header_ref.as_bytes(), b"one");
        assert_eq!(header_ref.to_str(), Some("one"));
        assert!(HeaderRef::try_from("a\rb").is_err());
        assert_eq!(HeaderRef::try_from("static").unwrap().as_bytes(), b"static");
    }

    #[test]
    fn typed_header_duplicate_singleton_is_rejected() {
        let mut map = HeaderMap::new();
        map.set(Host("first".to_owned()));
        map.set(Host("second".to_owned()));
        assert_eq!(map.get_all::<Host>().count(), 1);
        assert_eq!(map.get::<Host>().unwrap().0, "second");

        assert!(map.append(Host("third".to_owned())).is_err());
        assert_eq!(map.get_all::<Host>().count(), 1);

        map.remove::<Host>();
        map.append(Host("third".to_owned())).unwrap();
        assert!(map.append(Host("fourth".to_owned())).is_err());
        assert_eq!(map.get_all::<Host>().count(), 1);

        map.append(SetCookie("a=1".to_owned())).unwrap();
        map.append(SetCookie("b=2".to_owned())).unwrap();
        assert_eq!(map.get_all::<SetCookie>().count(), 2);
        assert_eq!(
            map.get_all::<SetCookie>()
                .map(|cookie| cookie.0.as_str())
                .collect::<Vec<_>>(),
            ["a=1", "b=2"]
        );

        map.remove::<SetCookie>();
        assert!(map.get::<SetCookie>().is_none());
        assert_eq!(map.get::<Host>().unwrap().0, "third");
    }

    #[test]
    fn raw_headers_are_ordered_lowercased_and_separate_from_typed() {
        let mut map = HeaderMap::new();
        map.append_raw("X-Custom", "one").unwrap();
        map.append_raw("x-custom", "two").unwrap();
        map.set(Host("example.com".to_owned()));

        assert_eq!(map.get_raw("X-CUSTOM").unwrap().as_bytes(), b"one");
        assert_eq!(map.get_raw("host"), None);
        let raw = map
            .iter_raw()
            .map(|(name, value)| (name.as_str().to_owned(), value.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        assert_eq!(
            raw,
            [
                ("x-custom".to_owned(), b"one".to_vec()),
                ("x-custom".to_owned(), b"two".to_vec())
            ]
        );

        assert!(map.append_raw("bad name", "value").is_err());
        assert!(map.append_raw("x-bad", "a\rb").is_err());
    }

    #[test]
    fn static_constructors_validate_and_typed_headers_roundtrip() {
        assert_eq!(HeaderName::from_static("host").unwrap(), HeaderName::HOST);
        assert!(HeaderName::from_static("bad name").is_err());
        assert_eq!(
            HeaderValue::from_static("text/plain").unwrap().as_bytes(),
            b"text/plain"
        );
        assert!(HeaderValue::from_static("a\rb").is_err());

        let host = Host::decode(HeaderRef::try_from("example.com").unwrap()).unwrap();
        assert_eq!(
            host.encode().unwrap(),
            HeaderValue::try_from("example.com").unwrap()
        );
    }

    #[test]
    fn header_error_display_is_stable() {
        assert_eq!(
            HeaderError::invalid_name().to_string(),
            "invalid header name"
        );
        assert_eq!(
            HeaderError::invalid_value().to_string(),
            "invalid header value"
        );
        assert_eq!(HeaderError::decode("bad date").to_string(), "bad date");
        assert_eq!(HeaderError::encode("bad date").to_string(), "bad date");
    }

    #[test]
    fn header_map_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HeaderMap>();
        assert_send_sync::<HeaderName>();
        assert_send_sync::<HeaderValue>();
        assert_send_sync::<HeaderRef<'static>>();
    }
}
