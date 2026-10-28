//! HTTP field names, values, maps and the standard typed header catalogue.

mod error;
mod map;
mod name;
mod registry;
mod standard;
mod value;

pub use error::{HeaderError, HeaderErrorKind};
pub use map::{Header, HeaderMap};
pub use name::{AsHeaderName, HeaderName, IntoHeaderName};
pub use standard::*;
pub use value::{HeaderRef, HeaderValue, IntoHeaderValue};

pub(crate) use map::TypedHeader;
pub(crate) use registry::HeaderRegistry;
