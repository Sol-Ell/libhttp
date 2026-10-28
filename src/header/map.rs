use std::any::{Any, TypeId};
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use super::HeaderRegistry;
use super::{
    AsHeaderName, HeaderError, HeaderName, HeaderRef, HeaderValue, IntoHeaderName, IntoHeaderValue,
};

/// A typed HTTP header known to this crate or registered by the application.
///
/// Implementations are decoded once per incoming field line and encoded once per outgoing field.
/// `MULTIPLE` controls whether the field may repeat; duplicate singleton fields are rejected.
pub trait Header: Sized + Send + Sync + 'static {
    /// The field name.
    fn name() -> HeaderName;

    /// Whether the field may appear more than once.
    const MULTIPLE: bool = false;

    /// Decodes one incoming field line.
    fn decode(value: HeaderRef<'_>) -> Result<Self, HeaderError>;

    /// Encodes this value as one outgoing field line.
    fn encode(&self) -> Result<HeaderValue, HeaderError>;
}

pub(crate) trait TypedHeader: Send + Sync {
    fn header_type_id(&self) -> TypeId;
    fn as_any(&self) -> &dyn Any;
    fn encode_value(&self) -> Result<HeaderValue, HeaderError>;
    fn type_name(&self) -> &'static str;
}

impl<T: Header> TypedHeader for T {
    fn header_type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn encode_value(&self) -> Result<HeaderValue, HeaderError> {
        self.encode()
    }

    fn type_name(&self) -> &'static str {
        std::any::type_name::<T>()
    }
}

enum Entry {
    Typed {
        name: HeaderName,
        value: Arc<dyn TypedHeader>,
    },
    Raw {
        name: HeaderName,
        value: HeaderValue,
    },
}

impl Clone for Entry {
    fn clone(&self) -> Self {
        match self {
            Self::Typed { name, value } => Self::Typed {
                name: name.clone(),
                value: value.clone(),
            },
            Self::Raw { name, value } => Self::Raw {
                name: name.clone(),
                value: value.clone(),
            },
        }
    }
}

impl Entry {
    fn name(&self) -> &HeaderName {
        match self {
            Self::Typed { name, .. } | Self::Raw { name, .. } => name,
        }
    }
}

/// An ordered, duplicate-preserving HTTP field map with typed and raw entries.
///
/// Typed entries are created by [`HeaderMap::set`]/[`HeaderMap::append`] or by decoding incoming
/// fields; raw entries are created by the parser or [`HeaderMap::append_raw`]. Lookup is
/// case-insensitive. [`HeaderMap::iter_raw`] exposes raw entries only; typed values are encoded
/// when a message is serialized.
#[derive(Default)]
pub struct HeaderMap {
    entries: Vec<Entry>,
}

impl Clone for HeaderMap {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
        }
    }
}

impl HeaderMap {
    /// Creates an empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty map with preallocated entry storage.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }

    /// Returns the number of stored entries, typed and raw.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the map is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Removes every entry.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Returns the first typed value of type `T`.
    pub fn get<T: Header>(&self) -> Option<&T> {
        self.entries.iter().find_map(|entry| match entry {
            Entry::Typed { value, .. } if value.header_type_id() == TypeId::of::<T>() => {
                value.as_any().downcast_ref::<T>()
            }
            _ => None,
        })
    }

    /// Returns every typed value of type `T`, in insertion order.
    pub fn get_all<T: Header>(&self) -> impl Iterator<Item = &T> {
        TypedIter::<T> {
            entries: self.entries.iter(),
            marker: PhantomData,
        }
    }

    /// Replaces every value of type `T` with `value`.
    pub fn set<T: Header>(&mut self, value: T) {
        let name = T::name();
        self.remove_name(&name);
        self.push_typed(name, Arc::new(value));
    }

    /// Appends a typed value, rejecting a duplicate singleton.
    pub fn append<T: Header>(&mut self, value: T) -> Result<(), HeaderError> {
        let name = T::name();
        if !T::MULTIPLE && self.entries.iter().any(|entry| entry.name() == &name) {
            return Err(HeaderError::duplicate(name.as_str()));
        }
        self.push_typed(name, Arc::new(value));
        Ok(())
    }

    /// Removes all values of type `T` and any raw field with its name.
    pub fn remove<T: Header>(&mut self) {
        self.remove_name(&T::name());
    }

    /// Returns the first raw value with `name`, or `None` if the field is typed or absent.
    pub fn get_raw(&self, name: impl AsHeaderName) -> Option<HeaderRef<'_>> {
        let name = name.as_header_name();
        self.entries.iter().find_map(|entry| match entry {
            Entry::Raw { name: field, value } if field.as_str().eq_ignore_ascii_case(name) => {
                Some(HeaderRef::from(value))
            }
            _ => None,
        })
    }

    /// Appends a raw field. Raw fields are not checked for singleton duplication.
    pub fn append_raw(
        &mut self,
        name: impl IntoHeaderName,
        value: impl IntoHeaderValue,
    ) -> Result<(), HeaderError> {
        let name = name.try_into_header_name()?;
        let value = value.try_into_header_value()?;
        self.append_raw_owned(name, value);
        Ok(())
    }

    /// Iterates over raw entries only.
    pub fn iter_raw(&self) -> impl Iterator<Item = (&HeaderName, HeaderRef<'_>)> {
        RawIter {
            entries: self.entries.iter(),
        }
    }

    /// Removes every typed and raw entry with `name`.
    pub fn remove_header(&mut self, name: impl AsHeaderName) -> bool {
        let name = name.as_header_name();
        let before = self.entries.len();
        self.entries
            .retain(|entry| !entry.name().as_str().eq_ignore_ascii_case(name));
        self.entries.len() != before
    }

    /// Whether any typed or raw entry has `name`.
    pub fn contains(&self, name: impl AsHeaderName) -> bool {
        let name = name.as_header_name();
        self.entries
            .iter()
            .any(|entry| entry.name().as_str().eq_ignore_ascii_case(name))
    }

    pub(crate) fn append_raw_owned(&mut self, name: HeaderName, value: HeaderValue) {
        self.entries.push(Entry::Raw { name, value });
    }

    pub(crate) fn push_typed(&mut self, name: HeaderName, value: Arc<dyn TypedHeader>) {
        self.entries.push(Entry::Typed { name, value });
    }

    pub(crate) fn has_typed(&self, type_id: TypeId) -> bool {
        self.entries.iter().any(|entry| match entry {
            Entry::Typed { value, .. } => value.header_type_id() == type_id,
            Entry::Raw { .. } => false,
        })
    }

    pub(crate) fn raw_headers(&self) -> Vec<(HeaderName, HeaderValue)> {
        self.entries
            .iter()
            .filter_map(|entry| match entry {
                Entry::Raw { name, value } => Some((name.clone(), value.clone())),
                Entry::Typed { .. } => None,
            })
            .collect()
    }

    /// Decodes registered typed values from the raw entries, preserving the raw entries.
    pub(crate) fn decode_registered(
        &mut self,
        registry: &HeaderRegistry,
    ) -> Result<(), HeaderError> {
        let raw = self.raw_headers();
        for (name, value) in raw {
            let Some(decoder) = registry.decoder(&name) else {
                continue;
            };
            if !decoder.multiple() && self.has_typed(decoder.type_id()) {
                return Err(HeaderError::duplicate(name.as_str()));
            }
            let decoded = registry.decode(decoder, HeaderRef::from(&value))?;
            self.push_typed(name, decoded);
        }
        Ok(())
    }

    /// Serializes every entry, in order, as header lines (without the final blank line).
    pub(crate) fn encode_into(&self, out: &mut Vec<u8>) -> Result<(), HeaderError> {
        self.encode_into_without(out, &[])
    }

    /// Serializes every entry except library-managed framing fields.
    pub(crate) fn encode_into_without(
        &self,
        out: &mut Vec<u8>,
        skip: &[&str],
    ) -> Result<(), HeaderError> {
        for entry in &self.entries {
            if skip
                .iter()
                .any(|name| entry.name().as_str().eq_ignore_ascii_case(name))
            {
                continue;
            }
            match entry {
                Entry::Raw { name, value } => {
                    write_field(out, name, value.as_bytes());
                }
                Entry::Typed { name, value } => {
                    let encoded = value.encode_value()?;
                    write_field(out, name, encoded.as_bytes());
                }
            }
        }
        Ok(())
    }

    fn remove_name(&mut self, name: &HeaderName) {
        self.entries.retain(|entry| entry.name() != name);
    }
}

fn write_field(out: &mut Vec<u8>, name: &HeaderName, value: &[u8]) {
    out.extend_from_slice(name.as_str().as_bytes());
    out.extend_from_slice(b": ");
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
}

impl fmt::Debug for HeaderMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        for entry in &self.entries {
            match entry {
                Entry::Raw { name, value } => {
                    map.entry(&name.as_str(), &String::from_utf8_lossy(value.as_bytes()));
                }
                Entry::Typed { name, value } => {
                    map.entry(&name.as_str(), &format_args!("<{}>", value.type_name()));
                }
            }
        }
        map.finish()
    }
}

struct TypedIter<'a, T> {
    entries: std::slice::Iter<'a, Entry>,
    marker: PhantomData<fn() -> T>,
}

impl<'a, T: Header> Iterator for TypedIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        for entry in self.entries.by_ref() {
            if let Entry::Typed { value, .. } = entry {
                if value.header_type_id() == TypeId::of::<T>() {
                    return value.as_any().downcast_ref::<T>();
                }
            }
        }
        None
    }
}

struct RawIter<'a> {
    entries: std::slice::Iter<'a, Entry>,
}

impl<'a> Iterator for RawIter<'a> {
    type Item = (&'a HeaderName, HeaderRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        for entry in self.entries.by_ref() {
            if let Entry::Raw { name, value } = entry {
                return Some((name, HeaderRef::from(value)));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Host, SetCookie};
    use super::*;

    #[test]
    fn set_replaces_and_raw_is_separate() {
        let mut map = HeaderMap::new();
        map.append_raw("Host", "example.com").unwrap();
        map.set(Host::new("other.example"));
        assert_eq!(map.get::<Host>().unwrap().as_str(), "other.example");
        assert!(map.get_raw("host").is_none());
    }

    #[test]
    fn singleton_append_is_rejected_but_multiple_is_allowed() {
        let mut map = HeaderMap::new();
        map.set(Host::new("a"));
        assert!(map.append(Host::new("b")).is_err());

        map.append(SetCookie::from_pair("a", "1")).unwrap();
        map.append(SetCookie::from_pair("b", "2")).unwrap();
        assert_eq!(map.get_all::<SetCookie>().count(), 2);
    }

    #[test]
    fn raw_iteration_preserves_order_and_duplicates() {
        let mut map = HeaderMap::new();
        map.append_raw("x-one", "a").unwrap();
        map.append_raw("x-two", "b").unwrap();
        map.append_raw("x-one", "c").unwrap();
        let fields: Vec<_> = map
            .iter_raw()
            .map(|(name, value)| (name.as_str().to_owned(), value.to_str().unwrap().to_owned()))
            .collect();
        assert_eq!(
            fields,
            vec![
                ("x-one".into(), "a".into()),
                ("x-two".into(), "b".into()),
                ("x-one".into(), "c".into())
            ]
        );
    }

    #[test]
    fn map_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HeaderMap>();
    }
}
