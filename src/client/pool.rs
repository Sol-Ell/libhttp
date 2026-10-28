use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use super::conn::ClientConnection;

/// The pool key: scheme, host and port.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PoolKey {
    pub(crate) scheme: String,
    pub(crate) host: String,
    pub(crate) port: u16,
}

/// Client connection pool limits.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    pub max_connections: usize,
    pub max_inflight_calls: usize,
    pub max_queued_body_bytes: usize,
    pub max_idle_connections: usize,
    pub max_idle_per_host: usize,
    pub max_connections_per_host: usize,
    pub max_cached_hosts: usize,
    pub max_queued_calls: usize,
    pub queue_timeout: Duration,
    pub idle_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 1_024,
            max_inflight_calls: 4_096,
            max_queued_body_bytes: 64 * 1024 * 1024,
            max_idle_connections: 256,
            max_idle_per_host: 4,
            max_connections_per_host: 8,
            max_cached_hosts: 1_024,
            max_queued_calls: 1_024,
            queue_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
        }
    }
}

struct Idle {
    conn: ClientConnection,
    since: Instant,
}

pub(crate) struct Pool {
    config: PoolConfig,
    idle: HashMap<PoolKey, VecDeque<Idle>>,
    idle_count: usize,
    active: usize,
    host_counts: HashMap<PoolKey, usize>,
}

impl Pool {
    pub(crate) fn new(config: PoolConfig) -> Self {
        Self {
            config,
            idle: HashMap::new(),
            idle_count: 0,
            active: 0,
            host_counts: HashMap::new(),
        }
    }

    pub(crate) fn config(&self) -> &PoolConfig {
        &self.config
    }

    pub(crate) fn checkout(&mut self, key: &PoolKey, now: Instant) -> Option<ClientConnection> {
        let idle = self.idle.get_mut(key)?.pop_front()?;
        self.idle_count -= 1;
        if now.duration_since(idle.since) > self.config.idle_timeout {
            self.drop_host(key);
            if self.idle.get(key).is_some_and(VecDeque::is_empty) {
                self.idle.remove(key);
            }
            return None;
        }
        self.active += 1;
        Some(idle.conn)
    }

    /// Reserves a slot for a new connection.
    pub(crate) fn reserve(&mut self, key: &PoolKey) -> bool {
        if self.idle_count + self.active >= self.config.max_connections {
            return false;
        }
        let host_count = self.host_counts.get(key).copied().unwrap_or(0);
        if host_count >= self.config.max_connections_per_host {
            return false;
        }
        self.active += 1;
        *self.host_counts.entry(key.clone()).or_insert(0) += 1;
        true
    }

    /// Releases a checkout that failed before becoming reusable.
    pub(crate) fn release(&mut self, key: &PoolKey) {
        self.active = self.active.saturating_sub(1);
        self.drop_host(key);
    }

    /// Returns a healthy, keep-alive connection to the idle set.
    pub(crate) fn checkin(&mut self, key: PoolKey, conn: ClientConnection) {
        self.active = self.active.saturating_sub(1);
        let host_count = self.host_counts.get(&key).copied().unwrap_or(0);
        if !conn.keep_alive
            || self.idle_count >= self.config.max_idle_connections
            || host_count > self.config.max_idle_per_host
        {
            self.drop_host(&key);
            return;
        }
        self.idle.entry(key).or_default().push_back(Idle {
            conn,
            since: Instant::now(),
        });
        self.idle_count += 1;
    }

    pub(crate) fn active(&self) -> usize {
        self.active
    }

    pub(crate) fn total(&self) -> usize {
        self.idle_count + self.active
    }

    pub(crate) fn clear(&mut self) {
        self.idle.clear();
        self.idle_count = 0;
        self.active = 0;
        self.host_counts.clear();
    }

    fn drop_host(&mut self, key: &PoolKey) {
        if let Some(count) = self.host_counts.get_mut(key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.host_counts.remove(key);
            }
        }
    }
}
