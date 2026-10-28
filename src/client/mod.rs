#![allow(clippy::type_complexity)]
//! The synchronous HTTP/1.1 client.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::body::{BodyChannel, BodyWriter, NoopWaker, PinnedBudget};
use crate::header::{Header, HeaderRegistry};
use crate::runtime::Runtime;
use crate::tls::TlsClientConfig;
use crate::{CancelToken, Error, Limits, Timeouts, Uri};

mod conn;
mod pool;
mod request;
mod response;

pub use pool::PoolConfig;
pub use request::ClientRequest;
pub use response::ClientResponse;

use conn::{ClientConnection, RequestSink, connection_key};
use pool::Pool;

/// A relaxed client counter snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClientStats {
    pub calls_started: u64,
    pub calls_active: usize,
    pub connections: usize,
    pub timeouts: u64,
    pub resets: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

#[derive(Default)]
struct ClientStatsInner {
    calls_started: AtomicU64,
    calls_active: AtomicUsize,
    timeouts: AtomicU64,
    resets: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
}

pub(crate) struct ClientInner {
    pub(crate) runtime: Runtime,
    tls: Option<TlsClientConfig>,
    timeouts: Timeouts,
    max_response_body_bytes: Option<u64>,
    registry: HeaderRegistry,
    pool: Mutex<Pool>,
    stats: ClientStatsInner,
    on_error: Option<Arc<dyn Fn(&Error) + Send + Sync>>,
    shutdown: AtomicBool,
}

impl ClientInner {
    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    pub(crate) fn report(&self, error: &Error) {
        if let Some(hook) = &self.on_error {
            hook(error);
        }
        if matches!(error, Error::Timeout(_)) {
            self.stats.timeouts.fetch_add(1, Ordering::Relaxed);
        }
        if matches!(error, Error::Canceled | Error::Closed) {
            self.stats.resets.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn return_connection(&self, conn: ClientConnection) {
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }
        let key = conn.key().clone();
        self.pool.lock().unwrap().checkin(key, conn);
    }

    fn acquire(&self, uri: &Uri) -> Result<(ClientConnection, bool), Error> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let key = connection_key(uri);
        let mut pool = self.pool.lock().unwrap();
        if pool.active() >= pool.config().max_inflight_calls {
            return Err(Error::Overloaded);
        }
        if let Some(conn) = pool.checkout(&key, std::time::Instant::now()) {
            return Ok((conn, true));
        }
        if !pool.reserve(&key) {
            return Err(Error::Overloaded);
        }
        drop(pool);
        match ClientConnection::connect(uri, self.tls.as_ref(), &self.timeouts) {
            Ok(conn) => Ok((conn, false)),
            Err(error) => {
                self.pool.lock().unwrap().release(&key);
                Err(error)
            }
        }
    }

    fn release(&self, conn: ClientConnection) {
        let key = conn.key().clone();
        self.pool.lock().unwrap().release(&key);
        drop(conn);
    }

    fn build_response(
        self: &Arc<Self>,
        conn: ClientConnection,
        head: crate::h1::ResponseHead,
    ) -> ClientResponse {
        let limits = Limits::default();
        let pinned = Arc::new(PinnedBudget::new(
            limits.max_pinned_bytes,
            limits.max_pinned_bytes_per_exchange,
        ));
        let channel = BodyChannel::outbound(
            limits.max_response_body_buffer,
            pinned,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(NoopWaker),
        );
        let trailers = Arc::new(std::sync::OnceLock::new());
        let peer = conn.peer();
        let local = conn.local();
        let secure = conn.is_secure();
        let response = ClientResponse {
            status: head.status,
            version: head.version,
            headers: head.headers,
            body: crate::body::BodyReader::new(channel.clone()),
            channel: channel.clone(),
            trailers: trailers.clone(),
            peer,
            local,
            secure,
        };
        conn.spawn_body_pump(
            channel,
            trailers,
            self.clone(),
            self.max_response_body_bytes,
        );
        response
    }

    fn send_buffered(self: &Arc<Self>, request: ClientRequest) -> Result<ClientResponse, Error> {
        self.stats.calls_started.fetch_add(1, Ordering::Relaxed);
        if request
            .cancel
            .as_ref()
            .is_some_and(CancelToken::is_canceled)
        {
            return Err(Error::Canceled);
        }
        let mut last_error = None;
        for attempt in 0..2 {
            let (mut conn, reused) = self.acquire(&request.uri)?;
            if let Err(error) = conn.send_buffered(&request) {
                self.release(conn);
                last_error = Some(error);
                if reused && attempt == 0 && request.method.is_idempotent() {
                    continue;
                }
                return Err(last_error.unwrap());
            }
            match conn.read_response_head(&self.registry) {
                Ok(head) => return Ok(self.build_response(conn, head)),
                Err(error) => {
                    self.release(conn);
                    last_error = Some(error);
                    if reused && attempt == 0 && request.method.is_idempotent() {
                        continue;
                    }
                    return Err(last_error.unwrap());
                }
            }
        }
        Err(last_error.unwrap_or(Error::Closed))
    }

    fn send_streaming(
        self: &Arc<Self>,
        request: ClientRequest,
        upload: Box<dyn FnOnce(&mut BodyWriter) -> Result<(), Error> + Send>,
    ) -> Result<ClientResponse, Error> {
        self.stats.calls_started.fetch_add(1, Ordering::Relaxed);
        if request
            .cancel
            .as_ref()
            .is_some_and(CancelToken::is_canceled)
        {
            return Err(Error::Canceled);
        }
        let (mut conn, _) = self.acquire(&request.uri)?;
        if let Err(error) = conn.send_chunked_head(&request) {
            self.release(conn);
            return Err(error);
        }
        let shared = Arc::new(Mutex::new(conn));
        let sink = Arc::new(RequestSink::new(shared.clone()));
        let mut writer = BodyWriter::new(sink);
        let mut upload_error = None;
        if let Err(error) = upload(&mut writer) {
            upload_error = Some(error);
        } else if let Err(error) = writer.finish() {
            upload_error = Some(error);
        }
        drop(writer);
        let mut conn = match Arc::try_unwrap(shared) {
            Ok(lock) => lock.into_inner().unwrap(),
            Err(_) => return Err(Error::Closed),
        };
        if let Some(error) = upload_error {
            // A server may answer before the upload completes. Give the response a short
            // chance to arrive; a real early response wins over the upload error.
            let _ = conn.set_read_timeout(Duration::from_millis(150));
            if let Ok(head) = conn.read_response_head(&self.registry) {
                return Ok(self.build_response(conn, head));
            }
            self.release(conn);
            return Err(error);
        }
        match conn.read_response_head(&self.registry) {
            Ok(head) => Ok(self.build_response(conn, head)),
            Err(error) => {
                self.release(conn);
                Err(error)
            }
        }
    }
}

/// A synchronous HTTP/1.1 client.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    /// Creates a builder with a private runtime.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub fn send(&self, request: ClientRequest) -> Result<ClientResponse, Error> {
        self.inner.send_buffered(request)
    }

    pub fn send_with<F>(&self, request: ClientRequest, body: F) -> Result<ClientResponse, Error>
    where
        F: FnOnce(&mut BodyWriter) -> Result<(), Error> + Send + 'static,
    {
        self.inner.send_streaming(request, Box::new(body))
    }

    pub fn fetch<F>(&self, request: ClientRequest, on_response: F) -> Result<(), Error>
    where
        F: for<'a> FnOnce(Result<&'a mut ClientResponse, Error>) -> Result<(), Error>
            + Send
            + 'static,
    {
        if self.inner.runtime.worker_count() == 0 {
            return Err(Error::InvalidConfig("fetch requires workers"));
        }
        let inner = self.inner.clone();
        let job = Box::new(move || match inner.send_buffered(request) {
            Ok(mut response) => {
                if let Err(error) = on_response(Ok(&mut response)) {
                    inner.report(&error);
                }
            }
            Err(error) => {
                inner.report(&error);
                let _ = on_response(Err(error));
            }
        });
        self.inner
            .runtime
            .dispatch_job(job)
            .map_err(|_| Error::Overloaded)
    }

    pub fn fetch_with<B, F>(
        &self,
        request: ClientRequest,
        upload: B,
        on_response: F,
    ) -> Result<(), Error>
    where
        B: FnOnce(&mut BodyWriter) -> Result<(), Error> + Send + 'static,
        F: for<'a> FnOnce(Result<&'a mut ClientResponse, Error>) -> Result<(), Error>
            + Send
            + 'static,
    {
        if self.inner.runtime.worker_count() == 0 {
            return Err(Error::InvalidConfig("fetch_with requires workers"));
        }
        let inner = self.inner.clone();
        let job = Box::new(
            move || match inner.send_streaming(request, Box::new(upload)) {
                Ok(mut response) => {
                    if let Err(error) = on_response(Ok(&mut response)) {
                        inner.report(&error);
                    }
                }
                Err(error) => {
                    inner.report(&error);
                    let _ = on_response(Err(error));
                }
            },
        );
        self.inner
            .runtime
            .dispatch_job(job)
            .map_err(|_| Error::Overloaded)
    }

    pub fn handle(&self) -> ClientHandle {
        ClientHandle {
            inner: self.inner.clone(),
        }
    }
}

/// A cloneable client control handle.
#[derive(Clone)]
pub struct ClientHandle {
    inner: Arc<ClientInner>,
}

impl ClientHandle {
    pub fn shutdown(&self) {
        self.shutdown_with_deadline(Duration::from_secs(30));
    }

    pub fn shutdown_with_deadline(&self, _grace: Duration) {
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.pool.lock().unwrap().clear();
    }

    pub fn stats(&self) -> ClientStats {
        let pool = self.inner.pool.lock().unwrap();
        ClientStats {
            calls_started: self.inner.stats.calls_started.load(Ordering::Relaxed),
            calls_active: self.inner.stats.calls_active.load(Ordering::Relaxed),
            connections: pool.total(),
            timeouts: self.inner.stats.timeouts.load(Ordering::Relaxed),
            resets: self.inner.stats.resets.load(Ordering::Relaxed),
            bytes_read: self.inner.stats.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.inner.stats.bytes_written.load(Ordering::Relaxed),
        }
    }

    pub fn runtime(&self) -> Runtime {
        self.inner.runtime.clone()
    }
}

/// Builds a [`Client`].
pub struct ClientBuilder {
    runtime: Option<Runtime>,
    tls: Option<TlsClientConfig>,
    pool: PoolConfig,
    timeouts: Timeouts,
    http1: bool,
    max_response_body_bytes: Option<u64>,
    registrations: Vec<fn(&mut HeaderRegistry) -> Result<(), crate::header::HeaderError>>,
    on_error: Option<Arc<dyn Fn(&Error) + Send + Sync>>,
}

fn register_header_fn<T: Header>(
    registry: &mut HeaderRegistry,
) -> Result<(), crate::header::HeaderError> {
    registry.register::<T>()
}

impl ClientBuilder {
    fn new() -> Self {
        Self {
            runtime: None,
            tls: None,
            pool: PoolConfig::default(),
            timeouts: Timeouts::default(),
            http1: true,
            max_response_body_bytes: None,
            registrations: Vec::new(),
            on_error: None,
        }
    }

    pub(crate) fn attached(runtime: Runtime) -> Self {
        let mut builder = Self::new();
        builder.runtime = Some(runtime);
        builder
    }

    pub fn tls(mut self, config: TlsClientConfig) -> Self {
        self.tls = Some(config);
        self
    }

    pub fn pool(mut self, config: PoolConfig) -> Self {
        self.pool = config;
        self
    }

    pub fn timeouts(mut self, timeouts: Timeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    pub fn http1(mut self, enabled: bool) -> Self {
        self.http1 = enabled;
        self
    }

    pub fn max_response_body_bytes(mut self, limit: Option<u64>) -> Self {
        self.max_response_body_bytes = limit;
        self
    }

    pub fn register_header<T: Header>(mut self) -> Self {
        self.registrations.push(register_header_fn::<T>);
        self
    }

    pub fn on_error(mut self, hook: Arc<dyn Fn(&Error) + Send + Sync>) -> Self {
        self.on_error = Some(hook);
        self
    }

    pub fn build(self) -> Result<Client, Error> {
        if !self.http1 {
            return Err(Error::InvalidConfig("HTTP/1.1 must be enabled in v1.0"));
        }
        self.timeouts.validate()?;
        if self.pool.max_connections == 0 || self.pool.max_inflight_calls == 0 {
            return Err(Error::InvalidConfig("client pool limits must be non-zero"));
        }
        let runtime = match self.runtime {
            Some(runtime) => runtime,
            None => Runtime::builder().build()?,
        };
        let mut registry = HeaderRegistry::standard();
        for register in self.registrations {
            register(&mut registry)
                .map_err(|_| Error::InvalidConfig("header registration name collision"))?;
        }
        Ok(Client {
            inner: Arc::new(ClientInner {
                runtime,
                tls: self.tls,
                timeouts: self.timeouts,
                max_response_body_bytes: self.max_response_body_bytes,
                registry,
                pool: Mutex::new(Pool::new(self.pool)),
                stats: ClientStatsInner::default(),
                on_error: self.on_error,
                shutdown: AtomicBool::new(false),
            }),
        })
    }
}
