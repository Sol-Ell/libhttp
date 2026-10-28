//! The synchronous HTTP/1.1 server.

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::body::PinnedBudget;
use crate::header::HeaderRegistry;
use crate::runtime::Runtime;
use crate::tls::TlsServerConfig;
use crate::{Error, Limits, Timeouts};

mod deferred;
mod request;
mod response;
pub(crate) mod stream;

pub use deferred::{DeferredFailure, DeferredFailureReason, DeferredRequest, DeferredResponse};
pub use request::Request;
pub use response::Response;

/// A synchronous request handler.
pub trait Handler: Send + Sync + 'static {
    fn handle(&self, request: &mut Request, response: &mut Response) -> Result<(), Error>;
}

impl<F> Handler for F
where
    F: Fn(&mut Request, &mut Response) -> Result<(), Error> + Send + Sync + 'static,
{
    fn handle(&self, request: &mut Request, response: &mut Response) -> Result<(), Error> {
        self(request, response)
    }
}

/// A cheap snapshot of server counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub connections: usize,
    pub requests: u64,
    pub failures: u64,
    pub timeouts: u64,
    pub resets: u64,
    pub deferred: usize,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

#[derive(Default)]
pub(crate) struct ServerStatsInner {
    pub(crate) connections: AtomicUsize,
    pub(crate) requests: AtomicU64,
    pub(crate) failures: AtomicU64,
    pub(crate) timeouts: AtomicU64,
    pub(crate) resets: AtomicU64,
    pub(crate) bytes_read: AtomicU64,
    pub(crate) bytes_written: AtomicU64,
}

impl ServerStatsInner {
    fn snapshot(&self, deferred: usize) -> Stats {
        Stats {
            connections: self.connections.load(Ordering::Relaxed),
            requests: self.requests.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            resets: self.resets.load(Ordering::Relaxed),
            deferred,
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
        }
    }
}

pub(crate) struct ServerShared {
    pub(crate) id: u64,
    pub(crate) runtime: Runtime,
    pub(crate) limits: Limits,
    pub(crate) timeouts: Timeouts,
    pub(crate) registry: HeaderRegistry,
    pub(crate) handler: OnceLock<Arc<dyn Handler>>,
    pub(crate) stats: Arc<ServerStatsInner>,
    pub(crate) active: AtomicUsize,
    pub(crate) shutdown: AtomicBool,
    pub(crate) grace: Mutex<Duration>,
    pub(crate) wait_mutex: Mutex<()>,
    pub(crate) wait_cond: Condvar,
    pub(crate) on_error: Option<Arc<dyn Fn(&Error) + Send + Sync>>,
    pub(crate) deferred: AtomicUsize,
    pub(crate) pinned: Arc<PinnedBudget>,
    pub(crate) local_addrs: Vec<SocketAddr>,
    pub(crate) tls: Option<TlsServerConfig>,
}

impl ServerShared {
    pub(crate) fn handler(&self) -> Arc<dyn Handler> {
        self.handler
            .get()
            .expect("handler is installed before listeners are registered")
            .clone()
    }

    pub(crate) fn notify(&self) {
        let _guard = self.wait_mutex.lock().unwrap();
        self.wait_cond.notify_all();
    }

    pub(crate) fn connection_open(&self) {
        self.active.fetch_add(1, Ordering::AcqRel);
        self.stats.connections.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn connection_closed(&self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
        self.notify();
    }

    pub(crate) fn report(&self, error: &Error) {
        if let Some(hook) = &self.on_error {
            hook(error);
        }
    }

    pub(crate) fn stats(&self) -> Stats {
        self.stats.snapshot(self.deferred.load(Ordering::Relaxed))
    }
}

type RegisterHeaderFn = fn(&mut HeaderRegistry) -> Result<(), crate::header::HeaderError>;

fn register_header_fn<T: crate::header::Header>(
    registry: &mut HeaderRegistry,
) -> Result<(), crate::header::HeaderError> {
    registry.register::<T>()
}

/// Builds a [`Server`].
pub struct ServerBuilder {
    runtime: Option<Runtime>,
    addrs: Vec<SocketAddr>,
    tls: Option<TlsServerConfig>,
    http1: bool,
    limits: Limits,
    timeouts: Timeouts,
    registrations: Vec<RegisterHeaderFn>,
    on_error: Option<Arc<dyn Fn(&Error) + Send + Sync>>,
}

impl ServerBuilder {
    pub(crate) fn new() -> Self {
        Self {
            runtime: None,
            addrs: Vec::new(),
            tls: None,
            http1: true,
            limits: Limits::default(),
            timeouts: Timeouts::default(),
            registrations: Vec::new(),
            on_error: None,
        }
    }

    pub(crate) fn attached(runtime: Runtime) -> Self {
        let mut builder = Self::new();
        builder.runtime = Some(runtime);
        builder
    }

    /// Adds a TCP address to bind. May be called more than once; port 0 is allowed.
    pub fn bind(mut self, address: impl ToSocketAddrs) -> Result<Self, Error> {
        let mut resolved = false;
        for address in address.to_socket_addrs()? {
            resolved = true;
            self.addrs.push(address);
        }
        if !resolved {
            return Err(Error::InvalidConfig("bind address did not resolve"));
        }
        Ok(self)
    }

    /// Enables TLS for this server.
    pub fn tls(mut self, config: TlsServerConfig) -> Self {
        self.tls = Some(config);
        self
    }

    /// Enables or disables HTTP/1.1. HTTP/1.1 is the only v1.0 protocol and must be enabled.
    pub fn http1(mut self, enabled: bool) -> Self {
        self.http1 = enabled;
        self
    }

    /// Replaces the server limits.
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Replaces the server timeouts.
    pub fn timeouts(mut self, timeouts: Timeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Registers an application header type for incoming decoding.
    pub fn register_header<T: crate::header::Header>(mut self) -> Self {
        self.registrations.push(register_header_fn::<T>);
        self
    }

    /// Observes reactor-level errors.
    pub fn on_error(mut self, hook: Arc<dyn Fn(&Error) + Send + Sync>) -> Self {
        self.on_error = Some(hook);
        self
    }

    /// Binds listeners and freezes the configuration.
    pub fn build(self) -> Result<Server, Error> {
        if !self.http1 {
            return Err(Error::InvalidConfig("HTTP/1.1 must be enabled in v1.0"));
        }
        if self.addrs.is_empty() {
            return Err(Error::InvalidConfig(
                "at least one bind address is required",
            ));
        }
        let runtime = match self.runtime {
            Some(runtime) => runtime,
            None => Runtime::builder().build()?,
        };
        if runtime.worker_count() == 0 {
            return Err(Error::InvalidConfig(
                "a server requires at least one worker",
            ));
        }
        self.limits.validate()?;
        self.timeouts.validate()?;
        let limits = self.limits.resolved(runtime.worker_count())?;
        let mut registry = HeaderRegistry::standard();
        for register in self.registrations {
            register(&mut registry)
                .map_err(|_| Error::InvalidConfig("header registration name collision"))?;
        }

        let mut listeners = Vec::with_capacity(self.addrs.len());
        let mut local_addrs = Vec::with_capacity(self.addrs.len());
        for address in self.addrs {
            let listener = mio::net::TcpListener::bind(address)?;
            let local = listener.local_addr()?;
            local_addrs.push(local);
            listeners.push(listener);
        }

        let id = runtime.next_server_id();
        let shared = Arc::new(ServerShared {
            id,
            runtime: runtime.clone(),
            limits: limits.clone(),
            timeouts: self.timeouts.clone(),
            registry,
            handler: OnceLock::new(),
            stats: Arc::new(ServerStatsInner::default()),
            active: AtomicUsize::new(0),
            shutdown: AtomicBool::new(false),
            grace: Mutex::new(self.timeouts.shutdown_grace),
            wait_mutex: Mutex::new(()),
            wait_cond: Condvar::new(),
            on_error: self.on_error,
            deferred: AtomicUsize::new(0),
            pinned: Arc::new(PinnedBudget::new(
                limits.max_pinned_bytes,
                limits.max_pinned_bytes_per_exchange,
            )),
            local_addrs,
            tls: self.tls,
        });
        Ok(Server { shared, listeners })
    }
}

/// A bound HTTP/1.1 server.
pub struct Server {
    shared: Arc<ServerShared>,
    listeners: Vec<mio::net::TcpListener>,
}

impl Server {
    /// Creates a builder with a private runtime.
    pub fn builder() -> ServerBuilder {
        ServerBuilder::new()
    }

    /// Returns a cloneable control handle.
    pub fn handle(&self) -> ServerHandle {
        ServerHandle {
            shared: self.shared.clone(),
        }
    }

    /// Registers listeners and blocks until shutdown. Consumes the server.
    pub fn run(self, handler: impl Handler) -> Result<(), Error> {
        let handler: Arc<dyn Handler> = Arc::new(handler);
        self.shared
            .handler
            .set(handler)
            .map_err(|_| Error::InvalidConfig("server already running"))?;
        self.shared
            .runtime
            .add_server(self.shared.clone(), self.listeners)?;
        let mut guard = self.shared.wait_mutex.lock().unwrap();
        while !self.shared.shutdown.load(Ordering::Acquire) {
            let (next, _) = self
                .shared
                .wait_cond
                .wait_timeout(guard, Duration::from_millis(250))
                .unwrap();
            guard = next;
        }
        drop(guard);
        let grace = *self.shared.grace.lock().unwrap();
        let deadline = Instant::now() + grace;
        loop {
            if self.shared.active.load(Ordering::Acquire) == 0 {
                break;
            }
            let now = Instant::now();
            if now >= deadline {
                self.shared
                    .runtime
                    .stop_server(self.shared.id, Duration::ZERO);
                break;
            }
            let guard = self.shared.wait_mutex.lock().unwrap();
            let (guard, _) = self
                .shared
                .wait_cond
                .wait_timeout(guard, deadline - now)
                .unwrap();
            drop(guard);
        }
        self.shared
            .runtime
            .stop_server(self.shared.id, grace.max(Duration::from_millis(1)));
        Ok(())
    }
}

/// A cloneable server control handle.
#[derive(Clone)]
pub struct ServerHandle {
    shared: Arc<ServerShared>,
}

impl ServerHandle {
    /// Signals shutdown and returns immediately.
    pub fn shutdown(&self) {
        let grace = *self.shared.grace.lock().unwrap();
        self.shutdown_with_deadline(grace);
    }

    /// Signals shutdown with a per-call grace period.
    pub fn shutdown_with_deadline(&self, grace: Duration) {
        *self.shared.grace.lock().unwrap() = grace;
        self.shared.shutdown.store(true, Ordering::Release);
        self.shared.notify();
        self.shared.runtime.stop_server(self.shared.id, grace);
    }

    /// Returns a relaxed counter snapshot.
    pub fn stats(&self) -> Stats {
        self.shared.stats()
    }

    /// Returns the owning runtime.
    pub fn runtime(&self) -> Runtime {
        self.shared.runtime.clone()
    }

    /// Returns the bound TCP addresses.
    pub fn local_addrs(&self) -> Vec<SocketAddr> {
        self.shared.local_addrs.clone()
    }
}
