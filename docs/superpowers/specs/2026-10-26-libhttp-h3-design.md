# libhttp h3 — Design

**Status:** subsystem design, rewritten after verification. Owns HTTP/3 framing, QPACK at capacity zero, the control streams, the connection demultiplexer and the QUIC boundary.
**Parent:** `2026-10-26-libhttp-design.md`.
**Consumes:** `conn` (`Conn`, `ConnHandle`, `Protocol`, `Event`, `Head`, `Body`, `StreamId`, `ConnError`, `CloseReason`, `codes`), transport (`UdpId`, `Datagram`, `OutDatagram`, `Chunk`).
**Consumed by:** `proxy`.

---

## 1. Scope

**Owns:** the `h3::Endpoint` demultiplexer, HTTP/3 frames and message semantics, SETTINGS, the three control streams, QPACK, GOAWAY, CONNECT, RFC 9221 datagrams, and the mapping from quinn-proto's events to `conn`'s.

**Does not own:** QUIC itself (quinn-proto), TLS configuration (`tls`), UDP sockets (transport), the envelope (`conn` §6).

**v1 omissions, stated once:** 0-RTT, connection migration, Alt-Svc, Happy Eyeballs, push. A server does not accept 0-RTT, and a client with early data available does not use it.

---

## 2. The QUIC boundary

quinn-proto owns QUIC: handshake, loss recovery, congestion control, stream and connection flow control. It owns no sockets, so the application moves datagrams on the `UdpId`.

Verified against 0.11.19:

| Direction | Call | Consequence |
|---|---|---|
| Inbound packet | `Endpoint::handle(now, remote, local_ip, ecn, BytesMut)` | one copy from the transport chunk into `BytesMut` |
| New connection | the event from `handle`, then accept | the server side is born before any handle exists |
| Egress packets | `Connection::poll_transmit(now, max_datagrams, &mut Vec<u8>)` | writes into caller storage; we copy the batch into a pooled chunk once |
| Stream read | `RecvStream::read(ordered) -> Chunks<'_>` | borrows the connection; finalised immediately (§7) |
| Stream write | the send-stream write | `Blocked` at a send limit, cleared by flow-control credit |
| Time | `poll_timeout` / `handle_timeout` | folded into the driver's heap via `Conn::arm` (`conn` §6.6), not a second clock |
| Datagrams | `Datagrams::send(Bytes, drop)` / `recv()` / `max_size()` | one copy each way; `Blocked` clears via `DatagramsUnblocked` |

The public API never exposes a quinn type, and every copy above is counted in §10 rather than called zero.

---

## 3. The demultiplexer

```rust
pub struct EndpointConfig {
    pub max_field_section_size: usize,   // advertised to peers
    pub datagrams: bool,                 // send SETTINGS_H3_DATAGRAM = 1
    pub stream_receive_window: u32,      // per-stream reservation
    pub receive_window: u64,             // must be >= the sum of stream windows
}

#[non_exhaustive]
pub enum Error {
    Conn(ConnError),          // anything the envelope reports
    Datagram,                 // malformed or unsupported datagram at the endpoint
    UnknownConnection,        // a handle the endpoint does not know
}

pub struct Endpoint { /* quinn endpoint + our per-connection map */ }
impl Endpoint {
    pub fn new(udp: UdpId, config: EndpointConfig) -> Result<Endpoint, Error>;
    pub fn connect(&mut self, remote: SocketAddr, server_name: ServerName) -> Result<h3::ConnId, Error>;
    pub fn feed(&mut self, datagram: Datagram<'_>) -> Result<(), Error>;
    pub fn poll(&mut self) -> Option<EndpointEvent>;
    pub fn accept(&mut self) -> Option<h3::ConnId>;
    pub fn conn(&mut self, id: h3::ConnId) -> Option<&mut Conn>;
    pub fn output_datagram(&mut self) -> Option<OutDatagram<'_>>;
}

pub enum EndpointEvent {
    Accepted(h3::ConnId),           // a client-initiated connection exists
    Connected(h3::ConnId),
    Closed(h3::ConnId, CloseReason),
}
```

`h3::ConnId` is generational and distinct from the transport's `ConnId`: one UDP socket carries many connections and none of them is a socket. That is the one place where the handle shape differs by protocol, and it is visible in the types.

**One owner for QUIC packets.** Inbound datagrams go to `Endpoint::feed`, never to `Conn::feed_datagram`; outbound packets come from `Endpoint::output_datagram`, never from `Conn::output_datagram`. `Conn::feed_datagram` and `Conn::output_datagram` exist in `conn` §6 for symmetry but are errors for an h3 connection — this document owns the packet path, and having two owners is how a packet gets sent twice. `Conn::tick`/`finish`/`poll` apply to h3 connections unchanged.

Discovery has exactly one path: a server calls `accept` after `Accepted`; a client gets `Connected` for the id `connect` returned. There is no second way to learn about a connection.

---

## 4. Streams and control streams

A request is one client-initiated bidirectional stream: `HEADERS`, optional `DATA`, optional trailing `HEADERS`.

A unidirectional stream begins with a variable-length type:

| Type | Stream | v1 |
|---|---|---|
| 0x00 | control | required; `SETTINGS` first |
| 0x01 | push | never opened; receiving push is `H3_ID_ERROR` |
| 0x02 | QPACK encoder | opened as a critical stream, carries nothing (§6) |
| 0x03 | QPACK decoder | opened as a critical stream, carries nothing (§6) |
| other | — | ignored, and the stream is not read further |

A critical stream closed by the peer is `H3_CLOSED_CRITICAL_STREAM`. A peer that opens a critical stream of the wrong type or twice is `H3_STREAM_CREATION_ERROR`.

The control stream carries `SETTINGS` first, then `GOAWAY`; `MAX_PUSH_ID` and `CANCEL_PUSH` are never sent. A second `SETTINGS`, or `SETTINGS` on any other stream, is `H3_FRAME_UNEXPECTED`. The three control streams' output is merged into connection order by the engine, and a blocked request stream cannot delay it (`conn` §6.4).

---

## 5. Settings

v1 sends `SETTINGS_QPACK_MAX_TABLE_CAPACITY = 0`, `SETTINGS_QPACK_BLOCKED_STREAMS = 0`, `SETTINGS_MAX_FIELD_SECTION_SIZE`, `ENABLE_CONNECT_PROTOCOL = 0`, and `SETTINGS_H3_DATAGRAM = 1` only when the caller enables datagrams.

Rules:

- A setting appearing twice is `H3_SETTINGS_ERROR`.
- An unknown setting is ignored — except the HTTP/2-reserved identifiers 0x02–0x05, which are `H3_SETTINGS_ERROR` (RFC 9114 §7.2.8).
- Missing `SETTINGS`, or any frame before it on the control stream, is `H3_MISSING_SETTINGS`.
- `SETTINGS_H3_DATAGRAM` is sent and checked symmetrically: datagrams are usable only if both sides advertised 1 (RFC 9221 §3). If the peer did not, `Conn::send_datagram` returns `ConnError::DatagramsUnsupported` and no `Event::Datagram` is ever produced. That check is not optional — without it the framing is silently wrong.

Because capacity is zero, the peer must not insert entries or send encoder instructions (RFC 9204 §3.2.3), so our decoder never resolves a dynamic reference and never blocks a stream. Our encoder is literal-only. That is the M5 deferral and the reason M3 is a framing milestone.

---

## 6. QPACK in v1

- Field sections are parsed in full: Required Insert Count, Base, the representation prefixes, Huffman, and the static table (99 entries). With capacity zero the required insert count is always 0; a peer that sends a dynamic reference is `QPACK_DECOMPRESSION_FAILED`.
- The encoder emits indexed-name or literal-name field lines with Huffman where shorter. No inserts, no duplicates, no dynamic references.
- **The encoder and decoder streams carry nothing in v1, in both directions.** We never insert, so we never owe a Section Acknowledgement, and we never receive one we must act on. The streams exist because RFC 9114 makes them critical, not because v1 has anything to put on them. An instruction arriving on the peer's QPACK encoder stream is `QPACK_ENCODER_STREAM_ERROR`; one on its decoder stream is likewise an error, because it acknowledges an insertion that never happened.

That asymmetry with h2's HPACK is deliberate: HPACK blocks arrive in order on the connection, so its dynamic table needs no per-block accounting; QPACK's do not, which is why its table is deferred and HPACK's is not.

---

## 7. Flow control and credit

QUIC's flow control is quinn's: `TransportConfig::{receive_window, stream_receive_window}`, configured to the per-stream reservation with the sum bounded like h2's. The engine reads at most one chunk ahead per stream, so the gap between "read" and "consumed" is bounded by one chunk plus the configured window.

Credit is released when `Chunks` is finalised or dropped. `Chunks` borrows the connection, so it cannot be held across an engine operation: the engine copies into a pooled chunk and finalises immediately, then turns the returned `ShouldTransmit` into output in the same `tick`. That is why `tick` runs before the output drain (`conn` §6.6).

This is the one place where the public "credit on consumption" rule (`conn` §4) is implemented at read time instead; the bound on a slow consumer is the configured window rather than a `Body` handle, and `Body.sink` is `None`. The reason is the borrow, not a preference.

---

## 8. Egress, writability and backpressure

A send-stream write returns `Blocked` at the stream or connection send limit and becomes writable again when flow-control credit arrives. The engine cannot hand quinn a pre-framed connection-ordered byte stream: it pushes frames into the transport and reports `Event::StreamWritable` when a stream can take more. `open` at the peer's stream limit returns `OpenError::WouldBlock`, and `Event::StreamsAvailable` is emitted when the limit rises.

`Endpoint::output_datagram` only drains `poll_transmit`. Pushing everything into quinn instead would make quinn's send buffer the real bound and blow the memory budget, so a blocked stream's bytes wait in the engine's bounded per-stream queue.

---

## 9. GOAWAY, shutdown, CONNECT

**Two-phase GOAWAY, the same shape as h2.** `Conn::stop_accepting` sends a GOAWAY with the maximum stream id (2^62 − 4), which asks the peer to stop opening without rejecting anything. `Conn::finish` requests the drain; the final GOAWAY, carrying the last accepted id, is emitted only when every accepted stream is terminal or the `Drain` deadline fires. `Drained` then follows `conn` §6.5's three conditions — this document does not restate them, because a weaker local version is how the FIN gets ahead of `close_notify`. Condition 2 is observable here because the endpoint owns the transmit queue: it calls `Conn::poll` only after `poll_transmit` returned `None`, so a `Drained` the engine emits cannot precede the last packet.

A peer GOAWAY is `Event::GoAway`; requests above its id were rejected and are safe to retry. `H3_REQUEST_REJECTED` is used only for admission refusal and above-GOAWAY requests, never for a stream that ran.

Plain `CONNECT` is opened with `StreamKind::Connect` (`conn` §5): `:method = CONNECT`, no `:scheme`/`:path`, extended CONNECT not advertised. A failed tunnel is `H3_CONNECT_ERROR`. The stream becomes a `Tunnel`, and its half-close is `END_STREAM` on that stream.

---

## 10. Datagrams (RFC 9221)

```rust
Conn::send_datagram(flow: StreamId, data: Chunk) -> Result<(), ConnError>
Conn::max_datagram_size(flow: StreamId) -> Option<usize>     // peer limit, MTU-capped
Event::Datagram { flow: StreamId, data: Chunk }
Event::DatagramWritable
```

- The `flow` becomes the Quarter Stream ID prefix on the wire, so a datagram is always associated with a request stream. A `flow` that is not an open request stream is `ConnError::StreamRefused`.
- `max_datagram_size` is `None` when the peer did not advertise support; `Some(n)` otherwise, capped by path MTU. A send above it is `ConnError::DatagramTooLarge`, never a silent drop.
- `Blocked` surfaces as `Event::DatagramWritable` on the connection, cleared by quinn's `DatagramsUnblocked`. There is no endpoint-level datagram event.
- Received datagrams carry no ECN and no address: a datagram frame can be coalesced into a packet with other frames, and quinn reports no packet metadata. The event reports only the flow and the bytes.
- **Batching is one datagram per call in v1.** The brief asks for that to be stated and measured rather than assumed, so the h3 gate records the transmit-batch size `poll_transmit` actually returns; multi-datagram batches are a phase-2 optimisation if the measurement justifies them.

Copies at this boundary: one per inbound packet, one per inbound body byte, one per outbound transmit batch, one per application datagram each way.

---

## 11. Message layer

The HTTP/3 message semantics are ours, not quinn's, and they mirror h2's with h3's codes:

- Pseudo-headers before regular fields; `:method`, `:scheme`, `:path` required for a request, `:authority` optional, `:status` for a response. `CONNECT` is the one exemption (`h2` §7, applied here). A violation is `H3_MESSAGE_ERROR`.
- Forbidden connection fields: `connection`, `keep-alive`, `proxy-connection`, `transfer-encoding`, `upgrade` — malformed. `te` is allowed only with `trailers`.
- Field validation: uppercase names, leading or trailing whitespace in a value, and NUL/CR/LF are malformed (RFC 9114 §4.2).
- `content-length`, when present, must equal the sum of `DATA`; a mismatch is `H3_MESSAGE_ERROR`.
- Trailers are a trailing `HEADERS` block; no pseudo-headers in it, no `DATA` after it, and a second trailer block is `H3_FRAME_UNEXPECTED`.

---

## 12. Errors

| Condition | Code |
|---|---|
| Malformed frame, frame in the wrong stream state | `H3_FRAME_UNEXPECTED` |
| Malformed frame payload | `H3_FRAME_ERROR` |
| Duplicate or reserved setting | `H3_SETTINGS_ERROR` |
| Missing `SETTINGS`, or a frame before it | `H3_MISSING_SETTINGS` |
| Wrong push id, push received | `H3_ID_ERROR` |
| A critical stream closed by the peer | `H3_CLOSED_CRITICAL_STREAM` |
| Critical stream of the wrong type, or twice | `H3_STREAM_CREATION_ERROR` |
| Pseudo-header or field-section violation | `H3_MESSAGE_ERROR` |
| Request above GOAWAY, or refused at admission | `H3_REQUEST_REJECTED` |
| Dispatched stream cancelled or timed out | `H3_REQUEST_CANCELLED` |
| Body cut before `END_STREAM` | `H3_REQUEST_INCOMPLETE` |
| Failed tunnel | `H3_CONNECT_ERROR` |
| Exceeding configured limits | `H3_EXCESSIVE_LOAD` |
| QPACK field section | `QPACK_DECOMPRESSION_FAILED` |
| QPACK encoder stream | `QPACK_ENCODER_STREAM_ERROR` |
| QPACK decoder stream | `QPACK_DECODER_STREAM_ERROR` |

Connection errors close the QUIC connection; stream errors reset one stream. The same "refused versus may have run" distinction as h2 applies, with different codes for the same reason.

---

## 13. What a builder decides

`max_field_section_size`, per-stream queue depth, when to coalesce `DATA` into one stream write, and whether `poll_transmit`'s GSO batching is used.

Not theirs: the capacity-zero QPACK cut, the control-stream types, the single packet owner, read-time credit and its reason, the datagram setting and Quarter Stream ID, and the two-phase GOAWAY.
