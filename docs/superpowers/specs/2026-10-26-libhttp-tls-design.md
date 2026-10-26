# libhttp tls — Design

**Status:** subsystem design. Owns the rustls integration: configuration, ALPN, the handshake, `close_notify`, buffer limits
and the copy cost.
**Parent:** `2026-10-26-libhttp-design.md`.
**Consumes:** `conn` (`feed`/`output` ordering), transport (`Chunk`).
**Consumed by:** `h1`, `h2`, and (through `rustls::quic`) `h3`.

---

## 1. Scope

**Owns:** `TlsConfig`, the `Tls` layer inside `Conn`, ALPN advertisement and resolution, certificate and root configuration, `close_notify`, buffer limits, and the measured copy cost.

**Does not own:** the handshake's *policy* (which ALPN list a server offers is `conn`'s `Protocol`), sockets (transport), or HTTP framing. It owns no thread, no socket and no event loop: rustls is sans-I/O and stays that way.

---

## 2. Types

```rust
pub struct TlsConfig {
    pub alpn: &'static [Version],     // the offered list; empty means no ALPN
    pub certs: Certs,                 // server chain + key, or none for a client
    pub roots: Roots,                 // feature `roots`, or an explicit verifier
    pub buffer_limit: usize,          // default 16 KiB; see §6
    pub max_early_data: usize,        // always 0 in v1
}

pub enum Certs { Chain(Arc<CertifiedKey>), None }
pub enum Roots { Webpki, Custom(Arc<RootCertStore>), None }

#[non_exhaustive]
pub enum Error {
    Alert(rustls::AlertDescription),
    TruncatedClose,                   // EOF without close_notify
    Handshake(Box<rustls::Error>),
    Alpn,                             // no overlap between offered and selected
    Io(std::io::Error),
}
```

---

## 3. Contracts

### 3.1 The handshake never blocks

Handshake bytes are ordinary ciphertext in the `feed`/`output` path (conn §6.2, §6.4). The layer never calls a socket and never waits; a handshake that needs more bytes simply produces no output and no event until `feed` supplies them. That is the entire reason for choosing a sans-I/O TLS library, and it is not restated as a property of `Conn`.

### 3.2 ALPN

The client offers exactly the list in `TlsConfig::alpn`; the server selects from it. `conn`'s `Protocol::Alpn` creates the engine from the selection, which happens during the handshake — before any application bytes exist, because 0-RTT is out of scope (early data is configured to zero and a 0-RTT packet is refused rather than accepted with unknown state).

No overlap is `Error::Alpn`, reported as a connection failure, not a 500.

### 3.3 close_notify and half-close

`close_notify` closes the **sender's write direction** and nothing else. It is the TLS half-close:

- `Conn::finish` (conn §6.5) queues it; the layer emits it as the last record once the engine has no more output.
- The peer's `close_notify` is detected by rustls and reported as the read side finishing. It does not make the connection unwritable, and it does not mean the socket died.
- `shutdown_write` (transport §4.5) is called only after `Drained`, so the FIN cannot overtake `close_notify`.

The half-close matrix, stated once:

| Leg | Half-close | Mechanism |
|---|---|---|
| plaintext h1 | yes | `shutdown(SHUT_WR)` |
| h1 over TLS | yes | `close_notify`, then `shutdown(SHUT_WR)`; reading continues |
| h2 / h3, per stream | yes | `END_STREAM` on that stream, connection untouched |
| h2 / h3, connection | graceful | GOAWAY then drain, not a socket half-close |

### 3.4 Truncation

EOF with no `close_notify` is `Error::TruncatedClose`, never a clean end. This is what makes `CloseReason::Peer` and a TLS failure distinguishable at the transport (transport §4.6): the layer reports which happened, and `conn` stops producing a clean `End` when the input ended mid-message.

Many peers do not send `close_notify`; the design still treats its absence as an error rather than silently widening the acceptance rule, because the alternative makes a truncation attack indistinguishable from a clean close.

### 3.5 No ambient trust

There are no ambient OS roots and no second TLS stack. `Roots::Webpki` is the opt-in bundled store (feature `roots`); `Roots::Custom` takes a store the application built; `Roots::None` is legal for a server and for a client with an out-of-band verifier, and fails closed otherwise.

### 3.6 QUIC

h3 uses the same rustls build through `rustls::quic` (the QUIC document owns that surface). No second TLS library is linked, and no C toolchain beyond the crypto provider is required — see the open decision in the umbrella.

---

## 4. Buffer limits

rustls has three buffers, and only two are governed by `set_buffer_limit`:

| Buffer | Default | Governed by | v1 setting |
|---|---|---|---|
| `received_plaintext` | 16 KiB | its own constant; `read_tls` refuses when full | 16 KiB (fixed) |
| `sendable_tls` | 64 KiB | `set_buffer_limit` | 16 KiB |
| `sendable_plaintext` | 64 KiB | `set_buffer_limit` | 16 KiB |

So a connection's TLS buffers cost 48 KiB configured, and 144 KiB at rustls' defaults. The 10k-connection figure in the umbrella uses the configured number; a build that forgets to call `set_buffer_limit` costs 1.37 GiB instead of 480 MiB, which is why the limit is set in `TlsConfig::buffer_limit` rather than left to the caller.

`received_plaintext` being outside `set_buffer_limit` is the one place where the configured total is not what the API suggests, and it is stated here so the memory budget is not a surprise.

---

## 5. Copy cost

| Path | Copies |
|---|---|
| Inbound | chunk → rustls' deframer (1), decrypt, plaintext → pooled chunk (1) |
| Outbound | plaintext → rustls' encrypt buffer (1), ciphertext → pooled chunk (1) |

That is up to two inbound and one outbound copy beyond the transport's own, and it is the cost of rustls' `read_tls`/`write_tls` shape. It is measured at M2. An in-place deframer is a named follow-up if the measurement is bad; it is not claimed here.

---

## 6. What a builder decides

The exact rustls provider wiring beyond `ring`, session-resumption policy, and whether the client's `ServerName` is validated before the handshake starts.

Not theirs: zero early data, ALPN resolution timing, the truncation rule, the half-close matrix, and the three buffer limits.
