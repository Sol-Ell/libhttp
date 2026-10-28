use std::any::TypeId;
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};

use super::{Header, HeaderError, HeaderName, HeaderRef, TypedHeader};

type DecodeFn = fn(HeaderRef<'_>) -> Result<std::sync::Arc<dyn TypedHeader>, HeaderError>;

pub(crate) struct Decoder {
    type_id: TypeId,
    multiple: bool,
    decode: DecodeFn,
}

impl Decoder {
    pub(crate) fn type_id(&self) -> TypeId {
        self.type_id
    }

    pub(crate) fn multiple(&self) -> bool {
        self.multiple
    }
}

/// The set of typed headers decoded from incoming messages.
#[derive(Default)]
pub(crate) struct HeaderRegistry {
    decoders: HashMap<HeaderName, Decoder>,
}

impl HeaderRegistry {
    /// Builds the registry containing every standard typed header.
    pub(crate) fn standard() -> Self {
        let mut registry = Self::default();
        macro_rules! register {
            ($($ty:ty),* $(,)?) => {
                $(registry
                    .register::<$ty>()
                    .expect("standard header registry has no name collisions");)*
            };
        }
        register!(
            super::Host,
            super::ContentLength,
            super::ContentType,
            super::ContentEncoding,
            super::ContentLanguage,
            super::ContentLocation,
            super::ContentRange,
            super::Date,
            super::Age,
            super::Expires,
            super::CacheControl,
            super::Pragma,
            super::Vary,
            super::ETag,
            super::LastModified,
            super::IfMatch,
            super::IfNoneMatch,
            super::IfModifiedSince,
            super::IfUnmodifiedSince,
            super::IfRange,
            super::Range,
            super::AcceptRanges,
            super::Connection,
            super::TransferEncoding,
            super::Te,
            super::Trailer,
            super::Expect,
            super::Cookie,
            super::SetCookie,
            super::Accept,
            super::AcceptEncoding,
            super::AcceptLanguage,
            super::Authorization,
            super::ProxyAuthorization,
            super::WwwAuthenticate,
            super::ProxyAuthenticate,
            super::Location,
            super::Referer,
            super::UserAgent,
            super::Server,
            super::RetryAfter,
            super::Allow,
            super::Link,
            super::Warning,
        );
        registry
    }

    /// Registers one typed header, rejecting a different type on the same name.
    pub(crate) fn register<T: Header>(&mut self) -> Result<(), HeaderError> {
        let name = T::name();
        let type_id = TypeId::of::<T>();
        if let Some(existing) = self.decoders.get(&name) {
            return if existing.type_id == type_id {
                Ok(())
            } else {
                Err(HeaderError::duplicate(name.as_str()))
            };
        }
        self.decoders.insert(
            name,
            Decoder {
                type_id,
                multiple: T::MULTIPLE,
                decode: decode::<T>,
            },
        );
        Ok(())
    }

    pub(crate) fn decoder(&self, name: &HeaderName) -> Option<&Decoder> {
        self.decoders.get(name)
    }

    pub(crate) fn decode(
        &self,
        decoder: &Decoder,
        value: HeaderRef<'_>,
    ) -> Result<std::sync::Arc<dyn TypedHeader>, HeaderError> {
        match catch_unwind(AssertUnwindSafe(|| (decoder.decode)(value))) {
            Ok(result) => result,
            Err(_) => Err(HeaderError::decode("header decoder panicked")),
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.decoders.len()
    }
}

fn decode<T: Header>(value: HeaderRef<'_>) -> Result<std::sync::Arc<dyn TypedHeader>, HeaderError> {
    T::decode(value).map(|value| std::sync::Arc::new(value) as std::sync::Arc<dyn TypedHeader>)
}

impl super::HeaderMap {
    /// Decodes all registered incoming fields into typed entries.
    pub(crate) fn decode_all(&mut self, registry: &HeaderRegistry) -> Result<(), HeaderError> {
        self.decode_registered(registry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::{Header, HeaderMap, HeaderName, HeaderRef, HeaderValue, Host};

    #[derive(Debug, PartialEq)]
    struct XCustom(String);

    impl Header for XCustom {
        fn name() -> HeaderName {
            HeaderName::from_static("x-custom")
        }

        fn decode(value: HeaderRef<'_>) -> Result<Self, HeaderError> {
            Ok(Self(value.to_str()?.to_owned()))
        }

        fn encode(&self) -> Result<HeaderValue, HeaderError> {
            HeaderValue::try_from(self.0.as_str())
        }
    }

    #[test]
    fn standard_registry_has_the_expected_headers() {
        let registry = HeaderRegistry::standard();
        assert!(registry.len() >= 40);
    }

    #[test]
    fn custom_registration_and_decoding() {
        let mut registry = HeaderRegistry::default();
        registry.register::<XCustom>().unwrap();
        let mut map = HeaderMap::new();
        map.append_raw("x-custom", "hello").unwrap();
        map.decode_all(&registry).unwrap();
        assert_eq!(map.get::<XCustom>().unwrap().0, "hello");
        assert!(map.get_raw("x-custom").is_some());
    }

    #[test]
    fn duplicate_singleton_decode_is_rejected() {
        let mut registry = HeaderRegistry::default();
        registry.register::<Host>().unwrap();
        let mut map = HeaderMap::new();
        map.append_raw("host", "a.example").unwrap();
        map.append_raw("host", "b.example").unwrap();
        assert!(map.decode_all(&registry).is_err());
    }

    #[test]
    fn registration_collision_is_rejected() {
        let mut registry = HeaderRegistry::default();
        registry.register::<Host>().unwrap();
        assert!(registry.register::<XCustom>().is_ok());
        // Different type, same name.
        #[derive(Debug)]
        struct XOther;
        impl Header for XOther {
            fn name() -> HeaderName {
                HeaderName::from_static("host")
            }
            fn decode(_: HeaderRef<'_>) -> Result<Self, HeaderError> {
                Ok(Self)
            }
            fn encode(&self) -> Result<HeaderValue, HeaderError> {
                Ok(HeaderValue::from_static(""))
            }
        }
        assert!(registry.register::<XOther>().is_err());
    }
}
