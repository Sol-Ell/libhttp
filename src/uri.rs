use std::fmt;

use url::Url;

/// An absolute `http` or `https` URI used by the client.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Uri {
    inner: Url,
}

/// The error returned when a client URI is malformed or unsupported.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidUri {
    detail: String,
}

impl Uri {
    /// Parses an absolute `http`/`https` URI without userinfo or a fragment.
    pub fn parse(value: &str) -> Result<Self, InvalidUri> {
        let inner = Url::parse(value).map_err(|error| InvalidUri {
            detail: error.to_string(),
        })?;
        if !matches!(inner.scheme(), "http" | "https") {
            return Err(InvalidUri {
                detail: "only http and https URIs are supported".to_owned(),
            });
        }
        if inner.host_str().is_none() {
            return Err(InvalidUri {
                detail: "URI has no host".to_owned(),
            });
        }
        if !inner.username().is_empty() || inner.password().is_some() {
            return Err(InvalidUri {
                detail: "userinfo is not supported".to_owned(),
            });
        }
        if inner.fragment().is_some() {
            return Err(InvalidUri {
                detail: "fragments are not sent in HTTP requests".to_owned(),
            });
        }
        Ok(Self { inner })
    }

    /// The scheme, `http` or `https`.
    pub fn scheme(&self) -> &str {
        self.inner.scheme()
    }

    /// The host, without brackets or port where possible.
    pub fn host(&self) -> &str {
        self.inner.host_str().unwrap_or("")
    }

    /// The explicit or default port.
    pub fn port(&self) -> Option<u16> {
        self.inner.port_or_known_default()
    }

    /// The explicit port, if the URI names one.
    pub fn explicit_port(&self) -> Option<u16> {
        self.inner.port()
    }

    /// The path, always beginning with `/`.
    pub fn path(&self) -> &str {
        self.inner.path()
    }

    /// The query, without the leading `?`.
    pub fn query(&self) -> Option<&str> {
        self.inner.query()
    }

    /// The path and query, suitable as an origin-form request target.
    pub fn path_and_query(&self) -> String {
        match self.inner.query() {
            Some(query) => format!("{}?{query}", self.inner.path()),
            None => self.inner.path().to_owned(),
        }
    }

    /// The full URI text.
    pub fn as_str(&self) -> &str {
        self.inner.as_str()
    }

    /// Whether this URI uses TLS.
    pub fn is_https(&self) -> bool {
        self.inner.scheme() == "https"
    }

    /// The `Host` field value: host plus a non-default port.
    pub(crate) fn host_header(&self) -> String {
        let host = self.host();
        let default_port = if self.is_https() { 443 } else { 80 };
        match self.inner.port() {
            Some(port) if port != default_port => format!("{host}:{port}"),
            _ => host.to_owned(),
        }
    }
}

impl std::str::FromStr for Uri {
    type Err = InvalidUri;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<&str> for Uri {
    type Error = InvalidUri;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<String> for Uri {
    type Error = InvalidUri;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl fmt::Display for Uri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for Uri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Uri").field(&self.as_str()).finish()
    }
}

impl fmt::Display for InvalidUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid URI: {}", self.detail)
    }
}

impl std::error::Error for InvalidUri {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_absolute_http_and_https() {
        let uri = Uri::parse("https://example.com:8443/a/b?x=1").unwrap();
        assert!(uri.is_https());
        assert_eq!(uri.host(), "example.com");
        assert_eq!(uri.port(), Some(8443));
        assert_eq!(uri.path_and_query(), "/a/b?x=1");
    }

    #[test]
    fn default_ports_are_elided_in_host_header() {
        let uri = Uri::parse("http://example.com:80/").unwrap();
        assert_eq!(uri.host_header(), "example.com");
        let uri = Uri::parse("http://example.com:8080/").unwrap();
        assert_eq!(uri.host_header(), "example.com:8080");
    }

    #[test]
    fn rejects_relative_userinfo_fragment_and_other_schemes() {
        assert!(Uri::parse("/relative").is_err());
        assert!(Uri::parse("ftp://example.com/").is_err());
        assert!(Uri::parse("http://user:pass@example.com/").is_err());
        assert!(Uri::parse("http://example.com/#x").is_err());
    }
}
