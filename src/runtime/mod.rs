//! The shared synchronous runtime: reactor threads, workers, timers and lifecycle.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam_channel::Sender;

use crate::Error;
use crate::server::{ServerBuilder, ServerShared};

mod exchange;
mod reactor;
mod slab;
mod timer;
mod worker;

pub(crate) use exchange::{Exchange, ResponseSink};
pub(crate) use reactor::{Command, ReactorHandle};

/// Runtime-wide configuration.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct RuntimeConfig {
    pub(crate) reactors: usize,
    pub(crate) workers: usize,
    pub(crate) resolver_threads: usize,
    pub(crate) max_client_callbacks: usize,
}

#[derive(Default)]
pub(crate) struct RuntimeStatsInner {
    pub(crate) active_connections: AtomicUsize,
    pub(crate) requests: AtomicU64,
}

/// A cheap runtime counter snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeStats {
    pub reactors: usize,
    pub workers: usize,
    pub active_connections: usize,
    pub requests: u64,
}

/// The thread-owning runtime shared by servers and clients.
#[derive(Clone)]
pub struct Runtime {
    pub(crate) inner: Arc<RuntimeInner>,
}

pub(crate) struct RuntimeInner {
    pub(crate) config: RuntimeConfig,
    pub(crate) reactors: Vec<ReactorHandle>,
    pub(crate) workers: Option<worker::WorkerPool>,
    pub(crate) worker_tx: Mutex<Option<Sender<worker::Job>>>,
    pub(crate) shutdown: AtomicBool,
    pub(crate) next_server: AtomicU64,
    pub(crate) stats: Arc<RuntimeStatsInner>,
}

impl Runtime {
    /// Creates a runtime builder.
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder::new()
    }

    /// Attaches a server builder to this runtime.
    pub fn server(&self) -> ServerBuilder {
        ServerBuilder::attached(self.clone())
    }

    /// Attaches a client builder to this runtime.
    pub fn client(&self) -> crate::client::ClientBuilder {
        crate::client::ClientBuilder::attached(self.clone())
    }

    /// Returns a cloneable control handle.
    pub fn handle(&self) -> RuntimeHandle {
        RuntimeHandle {
            inner: self.inner.clone(),
        }
    }

    pub(crate) fn worker_count(&self) -> usize {
        self.inner.config.workers
    }

    pub(crate) fn next_server_id(&self) -> u64 {
        self.inner.next_server.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn add_server(
        &self,
        shared: Arc<ServerShared>,
        listeners: Vec<mio::net::TcpListener>,
    ) -> Result<(), Error> {
        if self.inner.reactors.is_empty() {
            return Err(Error::InvalidConfig("runtime has no reactors"));
        }
        for (index, listener) in listeners.into_iter().enumerate() {
            let reactor = &self.inner.reactors[index % self.inner.reactors.len()];
            reactor.send(Command::AddListener {
                shared: shared.clone(),
                listener,
            });
        }
        Ok(())
    }

    pub(crate) fn dispatch_job(&self, job: worker::Job) -> Result<(), worker::Job> {
        let sender = self.inner.worker_tx.lock().unwrap().clone();
        match sender {
            Some(sender) => worker::try_dispatch(&sender, job),
            None => Err(job),
        }
    }

    pub(crate) fn stop_server(&self, server: u64, grace: std::time::Duration) {
        for reactor in &self.inner.reactors {
            reactor.send(Command::StopServer { server, grace });
        }
    }
}

/// Builds a [`Runtime`].
pub struct RuntimeBuilder {
    reactors: usize,
    workers: usize,
    resolver_threads: usize,
    max_client_callbacks: Option<usize>,
}

impl RuntimeBuilder {
    fn new() -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1);
        Self {
            reactors: parallelism.min(4),
            workers: parallelism,
            resolver_threads: 1,
            max_client_callbacks: None,
        }
    }

    /// Sets the number of reactor threads.
    pub fn reactors(mut self, n: usize) -> Self {
        self.reactors = n;
        self
    }

    /// Sets the number of worker threads. Zero is allowed only for blocking-only clients.
    pub fn workers(mut self, n: usize) -> Self {
        self.workers = n;
        self
    }

    /// Sets the number of DNS resolver threads.
    pub fn resolver_threads(mut self, n: usize) -> Self {
        self.resolver_threads = n;
        self
    }

    /// Caps concurrently executing client callbacks/uploads.
    pub fn max_client_callbacks(mut self, n: usize) -> Self {
        self.max_client_callbacks = Some(n);
        self
    }

    /// Starts the runtime.
    pub fn build(self) -> Result<Runtime, Error> {
        if self.reactors == 0 {
            return Err(Error::InvalidConfig("reactors must be non-zero"));
        }
        if self.resolver_threads == 0 {
            return Err(Error::InvalidConfig("resolver_threads must be non-zero"));
        }
        let max_client_callbacks = self
            .max_client_callbacks
            .unwrap_or_else(|| (self.workers / 2).max(1));
        if max_client_callbacks == 0 {
            return Err(Error::InvalidConfig(
                "max_client_callbacks must be non-zero",
            ));
        }
        let config = RuntimeConfig {
            reactors: self.reactors,
            workers: self.workers,
            resolver_threads: self.resolver_threads,
            max_client_callbacks,
        };
        let stats = Arc::new(RuntimeStatsInner::default());
        let (worker_tx, worker_rx) = if config.workers == 0 {
            (None, None)
        } else {
            let (tx, rx) = worker::channel(config.workers.saturating_mul(256).max(16));
            (Some(tx), Some(rx))
        };
        let mut reactors = Vec::with_capacity(config.reactors);
        for id in 0..config.reactors {
            reactors.push(reactor::Reactor::spawn(
                id,
                stats.clone(),
                worker_tx.clone(),
            )?);
        }
        let wakers: Vec<_> = reactors
            .iter()
            .map(|reactor| reactor.waker.clone())
            .collect();
        let workers = worker_rx.map(|rx| worker::WorkerPool::new(config.workers, rx, wakers));
        Ok(Runtime {
            inner: Arc::new(RuntimeInner {
                config,
                reactors,
                workers,
                worker_tx: Mutex::new(worker_tx),
                shutdown: AtomicBool::new(false),
                next_server: AtomicU64::new(1),
                stats,
            }),
        })
    }
}

/// A root runtime control handle.
#[derive(Clone)]
pub struct RuntimeHandle {
    inner: Arc<RuntimeInner>,
}

impl RuntimeHandle {
    /// Signals every reactor and worker to stop.
    pub fn shutdown(&self) {
        self.shutdown_with_deadline(std::time::Duration::from_secs(30));
    }

    /// Signals shutdown with a grace period.
    pub fn shutdown_with_deadline(&self, grace: std::time::Duration) {
        if self.inner.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        for reactor in &self.inner.reactors {
            reactor.send(Command::Shutdown { grace: Some(grace) });
        }
        if let Some(workers) = &self.inner.workers {
            workers.shutdown();
        }
        let _ = self.inner.worker_tx.lock().unwrap().take();
    }

    /// Waits for reactors and workers to finish.
    pub fn wait(&self) -> Result<(), Error> {
        for reactor in &self.inner.reactors {
            reactor.join();
        }
        let _ = self.inner.worker_tx.lock().unwrap().take();
        if let Some(workers) = &self.inner.workers {
            workers.join();
        }
        Ok(())
    }

    /// Returns a relaxed counter snapshot.
    pub fn stats(&self) -> RuntimeStats {
        RuntimeStats {
            reactors: self.inner.config.reactors,
            workers: self.inner.config.workers,
            active_connections: self.inner.stats.active_connections.load(Ordering::Relaxed),
            requests: self.inner.stats.requests.load(Ordering::Relaxed),
        }
    }
}

impl Drop for RuntimeInner {
    fn drop(&mut self) {
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        for reactor in &self.reactors {
            reactor.send(Command::Shutdown { grace: None });
        }
        for reactor in &self.reactors {
            reactor.join();
        }
        // Reactor threads have exited and dropped their sender clones.
        let _ = self.worker_tx.lock().unwrap().take();
        if let Some(workers) = &self.workers {
            workers.join();
        }
    }
}
