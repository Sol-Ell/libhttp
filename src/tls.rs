//! TLS configuration. The public API wraps rustls; rustls types stay private.

use std::sync::Arc;

use crate::Error;

#[cfg(feature = "tls")]
use std::io::Cursor;

#[cfg(feature = "tls")]
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
#[cfg(feature = "tls")]
use rustls::{ClientConfig, RootCertStore, ServerConfig};

#[cfg(feature = "tls")]
fn tls_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Tls(crate::TlsError::with_source(
        "TLS configuration error",
        error,
    ))
}

#[cfg(feature = "tls")]
fn parse_certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, Error> {
    rustls_pemfile::certs(&mut Cursor::new(pem))
        .collect::<Result<Vec<_>, _>>()
        .map_err(Error::Io)
}

#[cfg(feature = "tls")]
fn parse_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, Error> {
    rustls_pemfile::private_key(&mut Cursor::new(pem))
        .map_err(Error::Io)?
        .ok_or_else(|| Error::InvalidConfig("no private key in PEM input"))
}

/// Server-side TLS configuration.
#[derive(Clone)]
pub struct TlsServerConfig {
    #[cfg(feature = "tls")]
    pub(crate) inner: Arc<ServerConfig>,
    #[cfg(not(feature = "tls"))]
    pub(crate) inner: Arc<()>,
    #[cfg(feature = "tls")]
    certificate_pem: Vec<u8>,
    #[cfg(feature = "tls")]
    key_pem: Vec<u8>,
    #[cfg(feature = "tls")]
    client_roots: Option<Vec<u8>>,
    #[cfg(feature = "tls")]
    client_auth_required: bool,
}

impl TlsServerConfig {
    /// Builds a server configuration from a PEM certificate chain and private key.
    pub fn from_pem(certificate: &[u8], key: &[u8]) -> Result<Self, Error> {
        #[cfg(feature = "tls")]
        {
            let certs = parse_certs(certificate)?;
            let key_der = parse_key(key)?;
            let config = ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key_der)
                .map_err(tls_error)?;
            let mut config = config;
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            Ok(Self {
                inner: Arc::new(config),
                certificate_pem: certificate.to_vec(),
                key_pem: key.to_vec(),
                client_roots: None,
                client_auth_required: false,
            })
        }
        #[cfg(not(feature = "tls"))]
        {
            let _ = (certificate, key);
            Err(Error::InvalidConfig("this build has no TLS feature"))
        }
    }

    /// Builds a server configuration from PEM files.
    pub fn from_pem_files(
        certificate: impl AsRef<std::path::Path>,
        key: impl AsRef<std::path::Path>,
    ) -> Result<Self, Error> {
        let certificate = std::fs::read(certificate)?;
        let key = std::fs::read(key)?;
        Self::from_pem(&certificate, &key)
    }

    /// Requires or accepts client certificates signed by `roots_pem`.
    pub fn with_client_auth_roots(
        mut self,
        roots_pem: &[u8],
        required: bool,
    ) -> Result<Self, Error> {
        #[cfg(feature = "tls")]
        {
            let certs = parse_certs(&self.certificate_pem)?;
            let key_der = parse_key(&self.key_pem)?;
            let roots = parse_certs(roots_pem)?;
            let mut store = RootCertStore::empty();
            for cert in roots {
                store.add(cert).map_err(tls_error)?;
            }
            let mut verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(store));
            if !required {
                verifier = verifier.allow_unauthenticated();
            }
            let verifier = verifier.build().map_err(tls_error)?;
            let config = ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key_der)
                .map_err(tls_error)?;
            let mut config = config;
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            self.inner = Arc::new(config);
            self.client_roots = Some(roots_pem.to_vec());
            self.client_auth_required = required;
            Ok(self)
        }
        #[cfg(not(feature = "tls"))]
        {
            let _ = (roots_pem, required);
            Err(Error::InvalidConfig("this build has no TLS feature"))
        }
    }

    #[cfg(feature = "tls")]
    pub(crate) fn rustls_config(&self) -> Arc<ServerConfig> {
        self.inner.clone()
    }
}

/// Client-side TLS configuration.
#[derive(Clone)]
pub struct TlsClientConfig {
    #[cfg(feature = "tls")]
    pub(crate) inner: Arc<ClientConfig>,
    #[cfg(not(feature = "tls"))]
    pub(crate) inner: Arc<()>,
    #[cfg(feature = "tls")]
    roots_pem: Option<Vec<u8>>,
    #[cfg(feature = "tls")]
    client_cert: Option<(Vec<u8>, Vec<u8>)>,
    #[cfg(feature = "tls")]
    use_webpki_roots: bool,
}

#[cfg(feature = "tls")]
fn build_client_config(
    roots_pem: Option<&[u8]>,
    use_webpki_roots: bool,
    client_cert: Option<(&[u8], &[u8])>,
) -> Result<ClientConfig, Error> {
    let mut roots = RootCertStore::empty();
    if let Some(pem) = roots_pem {
        for cert in parse_certs(pem)? {
            roots.add(cert).map_err(tls_error)?;
        }
    } else if use_webpki_roots {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    let builder = ClientConfig::builder().with_root_certificates(roots);
    let mut config = match client_cert {
        Some((cert, key)) => builder
            .with_client_auth_cert(parse_certs(cert)?, parse_key(key)?)
            .map_err(tls_error)?,
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

impl TlsClientConfig {
    /// Verifies servers against the bundled webpki roots.
    pub fn with_webpki_roots() -> Self {
        #[cfg(feature = "tls")]
        {
            let config = build_client_config(None, true, None).expect("webpki roots are valid");
            Self {
                inner: Arc::new(config),
                roots_pem: None,
                client_cert: None,
                use_webpki_roots: true,
            }
        }
        #[cfg(not(feature = "tls"))]
        {
            Self {
                inner: Arc::new(()),
            }
        }
    }

    /// Verifies servers against PEM-encoded roots.
    pub fn with_roots_pem(roots_pem: &[u8]) -> Result<Self, Error> {
        #[cfg(feature = "tls")]
        {
            let config = build_client_config(Some(roots_pem), false, None)?;
            Ok(Self {
                inner: Arc::new(config),
                roots_pem: Some(roots_pem.to_vec()),
                client_cert: None,
                use_webpki_roots: false,
            })
        }
        #[cfg(not(feature = "tls"))]
        {
            let _ = roots_pem;
            Err(Error::InvalidConfig("this build has no TLS feature"))
        }
    }

    /// Adds a client certificate and key.
    pub fn with_client_certificate(
        mut self,
        certificate_pem: &[u8],
        key_pem: &[u8],
    ) -> Result<Self, Error> {
        #[cfg(feature = "tls")]
        {
            let config = build_client_config(
                self.roots_pem.as_deref(),
                self.use_webpki_roots,
                Some((certificate_pem, key_pem)),
            )?;
            self.inner = Arc::new(config);
            self.client_cert = Some((certificate_pem.to_vec(), key_pem.to_vec()));
            Ok(self)
        }
        #[cfg(not(feature = "tls"))]
        {
            let _ = (certificate_pem, key_pem);
            Err(Error::InvalidConfig("this build has no TLS feature"))
        }
    }

    #[cfg(feature = "tls")]
    pub(crate) fn rustls_config(&self) -> Arc<ClientConfig> {
        self.inner.clone()
    }
}
