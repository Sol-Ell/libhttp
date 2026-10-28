use std::time::Duration;

use crate::Error;

/// Server limits. Every queue and buffer has a bound and a defined reaction.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Maximum raw HTTP/1.1 request head size.
    pub max_head_bytes: u32,
    /// Maximum number of request header fields.
    pub max_header_count: u32,
    /// Maximum decoded request body size; `None` is unlimited.
    pub max_body_bytes: Option<u64>,
    /// Maximum simultaneous server connections.
    pub max_connections: usize,
    /// Maximum simultaneous connections per reactor.
    pub max_connections_per_reactor: usize,
    /// Maximum accepted sockets waiting for reactor registration.
    pub max_pending_accepts: usize,
    /// Maximum exchanges running on workers; `None` resolves to the worker count.
    pub max_inflight_exchanges: Option<usize>,
    /// Maximum exchanges waiting for a worker; `None` resolves to `workers * 256`.
    pub max_queued_exchanges: Option<usize>,
    /// Maximum in-flight exchanges per connection; `None` resolves to 1.
    pub max_inflight_exchanges_per_connection: Option<usize>,
    /// Maximum pipelined requests accepted per connection.
    pub max_pipelined_requests: u32,
    /// Per-exchange inbound body ring ceiling.
    pub max_request_body_buffer: usize,
    /// Per-exchange outbound body ring ceiling.
    pub max_response_body_buffer: usize,
    /// Maximum decoded trailer bytes.
    pub max_trailer_bytes: u32,
    /// Maximum typed-header arena bytes per head.
    pub max_header_arena_bytes: u32,
    /// Per-connection body/head buffer budget.
    pub max_connection_buffer_bytes: usize,
    /// Server-wide active buffer budget.
    pub max_buffered_bytes: usize,
    /// Server-wide pinned `BodyChunk` budget.
    pub max_pinned_bytes: usize,
    /// Per-exchange pinned `BodyChunk` budget.
    pub max_pinned_bytes_per_exchange: usize,
    /// Maximum workers parked on body I/O; `None` resolves to `max(1, workers / 2)`.
    pub max_parked_workers: Option<usize>,
    /// Maximum parked workers per connection; `None` resolves to half the global cap.
    pub max_parked_per_connection: Option<usize>,
    /// Maximum parked readers per exchange.
    pub max_parked_readers_per_exchange: usize,
    /// Maximum parked writers per exchange.
    pub max_parked_writers_per_exchange: usize,
    /// Maximum detached (deferred) exchanges per server.
    pub max_deferred_exchanges: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_head_bytes: 16 * 1024,
            max_header_count: 100,
            max_body_bytes: Some(8 * 1024 * 1024),
            max_connections: 100_000,
            max_connections_per_reactor: 50_000,
            max_pending_accepts: 1_024,
            max_inflight_exchanges: None,
            max_queued_exchanges: None,
            max_inflight_exchanges_per_connection: Some(1),
            max_pipelined_requests: 8,
            max_request_body_buffer: 64 * 1024,
            max_response_body_buffer: 64 * 1024,
            max_trailer_bytes: 8 * 1024,
            max_header_arena_bytes: 64 * 1024,
            max_connection_buffer_bytes: 256 * 1024,
            max_buffered_bytes: 256 * 1024 * 1024,
            max_pinned_bytes: 64 * 1024 * 1024,
            max_pinned_bytes_per_exchange: 128 * 1024,
            max_parked_workers: None,
            max_parked_per_connection: None,
            max_parked_readers_per_exchange: 1,
            max_parked_writers_per_exchange: 1,
            max_deferred_exchanges: 4_096,
        }
    }
}

impl Limits {
    pub(crate) fn resolved(&self, workers: usize) -> Result<Self, Error> {
        let mut limits = self.clone();
        let workers = workers.max(1);

        resolve_count(
            &mut limits.max_inflight_exchanges,
            workers,
            "max_inflight_exchanges",
        )?;
        resolve_count(
            &mut limits.max_queued_exchanges,
            workers.saturating_mul(256),
            "max_queued_exchanges",
        )?;
        resolve_count(
            &mut limits.max_inflight_exchanges_per_connection,
            1,
            "max_inflight_exchanges_per_connection",
        )?;
        resolve_count(
            &mut limits.max_parked_workers,
            workers.div_ceil(2).max(1),
            "max_parked_workers",
        )?;
        let parked = limits.max_parked_workers.unwrap_or(1);
        resolve_count(
            &mut limits.max_parked_per_connection,
            parked.div_ceil(2).max(1),
            "max_parked_per_connection",
        )?;

        limits.validate()?;
        Ok(limits)
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.max_head_bytes == 0 {
            return Err(Error::InvalidConfig("max_head_bytes must be non-zero"));
        }
        if self.max_header_count == 0 {
            return Err(Error::InvalidConfig("max_header_count must be non-zero"));
        }
        if self.max_connections == 0 {
            return Err(Error::InvalidConfig("max_connections must be non-zero"));
        }
        if self.max_connections_per_reactor == 0 {
            return Err(Error::InvalidConfig(
                "max_connections_per_reactor must be non-zero",
            ));
        }
        if self.max_pending_accepts == 0 {
            return Err(Error::InvalidConfig("max_pending_accepts must be non-zero"));
        }
        if self.max_pipelined_requests == 0 {
            return Err(Error::InvalidConfig(
                "max_pipelined_requests must be non-zero",
            ));
        }
        for (name, value) in [
            ("max_request_body_buffer", self.max_request_body_buffer),
            ("max_response_body_buffer", self.max_response_body_buffer),
            ("max_trailer_bytes", self.max_trailer_bytes as usize),
            (
                "max_header_arena_bytes",
                self.max_header_arena_bytes as usize,
            ),
            (
                "max_connection_buffer_bytes",
                self.max_connection_buffer_bytes,
            ),
            ("max_buffered_bytes", self.max_buffered_bytes),
            ("max_pinned_bytes", self.max_pinned_bytes),
            (
                "max_pinned_bytes_per_exchange",
                self.max_pinned_bytes_per_exchange,
            ),
            (
                "max_parked_readers_per_exchange",
                self.max_parked_readers_per_exchange,
            ),
            (
                "max_parked_writers_per_exchange",
                self.max_parked_writers_per_exchange,
            ),
            ("max_deferred_exchanges", self.max_deferred_exchanges),
        ] {
            if value == 0 {
                return Err(Error::InvalidConfig(match name {
                    "max_request_body_buffer" => "max_request_body_buffer must be non-zero",
                    "max_response_body_buffer" => "max_response_body_buffer must be non-zero",
                    "max_trailer_bytes" => "max_trailer_bytes must be non-zero",
                    "max_header_arena_bytes" => "max_header_arena_bytes must be non-zero",
                    "max_connection_buffer_bytes" => "max_connection_buffer_bytes must be non-zero",
                    "max_buffered_bytes" => "max_buffered_bytes must be non-zero",
                    "max_pinned_bytes" => "max_pinned_bytes must be non-zero",
                    "max_pinned_bytes_per_exchange" => {
                        "max_pinned_bytes_per_exchange must be non-zero"
                    }
                    "max_parked_readers_per_exchange" => {
                        "max_parked_readers_per_exchange must be non-zero"
                    }
                    "max_parked_writers_per_exchange" => {
                        "max_parked_writers_per_exchange must be non-zero"
                    }
                    "max_deferred_exchanges" => "max_deferred_exchanges must be non-zero",
                    _ => "limit must be non-zero",
                }));
            }
        }
        for (name, value) in [
            ("max_inflight_exchanges", self.max_inflight_exchanges),
            ("max_queued_exchanges", self.max_queued_exchanges),
            (
                "max_inflight_exchanges_per_connection",
                self.max_inflight_exchanges_per_connection,
            ),
            ("max_parked_workers", self.max_parked_workers),
            ("max_parked_per_connection", self.max_parked_per_connection),
        ] {
            if matches!(value, Some(0)) {
                return Err(Error::InvalidConfig(match name {
                    "max_inflight_exchanges" => "max_inflight_exchanges must be non-zero",
                    "max_queued_exchanges" => "max_queued_exchanges must be non-zero",
                    "max_inflight_exchanges_per_connection" => {
                        "max_inflight_exchanges_per_connection must be non-zero"
                    }
                    "max_parked_workers" => "max_parked_workers must be non-zero",
                    "max_parked_per_connection" => "max_parked_per_connection must be non-zero",
                    _ => "limit must be non-zero",
                }));
            }
        }
        Ok(())
    }
}

fn resolve_count(
    slot: &mut Option<usize>,
    default: usize,
    name: &'static str,
) -> Result<(), Error> {
    if let Some(value) = *slot {
        if value == 0 {
            return Err(Error::InvalidConfig(match name {
                "max_inflight_exchanges" => "max_inflight_exchanges must be non-zero",
                "max_queued_exchanges" => "max_queued_exchanges must be non-zero",
                "max_inflight_exchanges_per_connection" => {
                    "max_inflight_exchanges_per_connection must be non-zero"
                }
                "max_parked_workers" => "max_parked_workers must be non-zero",
                "max_parked_per_connection" => "max_parked_per_connection must be non-zero",
                _ => "limit must be non-zero",
            }));
        }
        return Ok(());
    }
    *slot = Some(default);
    Ok(())
}

/// Deadlines for server and client exchanges.
#[derive(Clone, Debug)]
pub struct Timeouts {
    /// Time allowed to read a complete request/response head.
    pub header_read: Duration,
    /// Maximum time between body bytes.
    pub body_idle: Duration,
    /// Maximum time a worker may wait for inbound body data.
    pub read_stall: Duration,
    /// Maximum time a worker may wait for outbound body capacity.
    pub write_stall: Duration,
    /// Maximum idle time between keep-alive requests.
    pub keep_alive: Duration,
    /// Maximum TLS handshake time.
    pub tls_handshake: Duration,
    /// Maximum client connect time per attempt.
    pub connect: Duration,
    /// Total request deadline; `None` disables it.
    pub total_request: Option<Duration>,
    /// Maximum lifetime of a deferred exchange; `None` disables it.
    pub deferred: Option<Duration>,
    /// Grace period for graceful shutdown.
    pub shutdown_grace: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            header_read: Duration::from_secs(10),
            body_idle: Duration::from_secs(30),
            read_stall: Duration::from_secs(15),
            write_stall: Duration::from_secs(15),
            keep_alive: Duration::from_secs(60),
            tls_handshake: Duration::from_secs(10),
            connect: Duration::from_secs(10),
            total_request: None,
            deferred: Some(Duration::from_secs(60)),
            shutdown_grace: Duration::from_secs(30),
        }
    }
}

impl Timeouts {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        for (name, value) in [
            ("header_read", self.header_read),
            ("body_idle", self.body_idle),
            ("read_stall", self.read_stall),
            ("write_stall", self.write_stall),
            ("keep_alive", self.keep_alive),
            ("tls_handshake", self.tls_handshake),
            ("connect", self.connect),
        ] {
            if value.is_zero() {
                return Err(Error::InvalidConfig(match name {
                    "header_read" => "header_read must be non-zero",
                    "body_idle" => "body_idle must be non-zero",
                    "read_stall" => "read_stall must be non-zero",
                    "write_stall" => "write_stall must be non-zero",
                    "keep_alive" => "keep_alive must be non-zero",
                    "tls_handshake" => "tls_handshake must be non-zero",
                    "connect" => "connect must be non-zero",
                    _ => "timeout must be non-zero",
                }));
            }
        }
        if matches!(self.total_request, Some(value) if value.is_zero()) {
            return Err(Error::InvalidConfig("total_request must be non-zero"));
        }
        if matches!(self.deferred, Some(value) if value.is_zero()) {
            return Err(Error::InvalidConfig("deferred must be non-zero"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_frozen_contract() {
        let limits = Limits::default();
        assert_eq!(limits.max_head_bytes, 16 * 1024);
        assert_eq!(limits.max_trailer_bytes, 8 * 1024);
        assert_eq!(limits.max_deferred_exchanges, 4_096);
        assert_eq!(limits.max_connection_buffer_bytes, 256 * 1024);
        assert_eq!(limits.max_body_bytes, Some(8 * 1024 * 1024));
    }

    #[test]
    fn worker_derived_limits_resolve() {
        let limits = Limits::default().resolved(4).unwrap();
        assert_eq!(limits.max_inflight_exchanges, Some(4));
        assert_eq!(limits.max_queued_exchanges, Some(1_024));
        assert_eq!(limits.max_parked_workers, Some(2));
        assert_eq!(limits.max_parked_per_connection, Some(1));
    }

    #[test]
    fn zero_limits_are_rejected() {
        let limits = Limits {
            max_head_bytes: 0,
            ..Limits::default()
        };
        assert!(matches!(limits.validate(), Err(Error::InvalidConfig(_))));

        let limits = Limits {
            max_inflight_exchanges: Some(0),
            ..Limits::default()
        };
        assert!(limits.resolved(1).is_err());
    }

    #[test]
    fn timeouts_validate() {
        assert!(Timeouts::default().validate().is_ok());
        let timeouts = Timeouts {
            header_read: Duration::ZERO,
            ..Timeouts::default()
        };
        assert!(timeouts.validate().is_err());
    }
}
