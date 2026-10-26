# libhttp h3 — Design

**Status:** subsystem design. Owns HTTP/3 framing, QPACK (capacity zero in v1), the control streams, the connection demultiplexer, and the QUIC boundary.
**Parent:** `2026-10-26-libhttp-design.md`.
**Consumes:** `conn` (`Event`, `Head`, `Body`, `StreamId`), transport (`UdpId`, `Datagram`, `Chunk`).
**Consumed by:** `proxy`.

---

## 1. Scope

**Owns:** the `h3::Endpoint` demultiplexer, HTTP/3 frames, SETTINGS, the three control streams, QPACK, `GOAWAY`, `CONNECT`, RFC 9221 datagrams, and the mapping between quinn-proto's events and `conn`'s.

**Does not own:** QUIC itself (quinn-proto), TLS configuration (`tls`), UDP sockets (transport), the event vocabulary (`conn`).

**v1 omissions, stated once:** 0-RTT, connection migration, Alt-Svc discovery, Happy Eyeballs, push. A server does not accept 0-RTT (`max_early_data = 0`), and a client with early data available does not use it.

---

## 2. The QUIC boundary

quinn-proto is sans-I/O and owns QUIC: handshake, loss recovery, congestion control, stream flow control. It owns no sockets, so the application (or `Server`) moves datagrams.

The boundary calls this design depends on, verified for 0.11.19:

| Direction | Call | Consequence |
|---|---|---|
| Inbound packet | `Endpoint::handle(now, remote, local_ip, ecn, BytesMut)` | one copy from the transport's chunk into `BytesMut`, because the API takes owned bytes |
| New connection | the event from `handle`, then accept with a transport config | the server side of a connection is born here, before any `ConnId` exists |
| Egress packets | `Connection::poll_transmit(now, max_datagrams, &mut Vec<u8>)` | the transport writes into caller-owned storage; we copy the batch into a pooled chunk once |
| Stream read | `RecvStream::read(ordered) -> Chunks<'_>` | `Chunks` borrows the connection and is `finalize()`d immediately (see §7) |
| Stream write | the send-stream write | `Blocked` at a send limit, cleared by flow-control credit |
| Time | `poll_timeout` / `handle_timeout` | the QUIC timer is folded into the driver's heap (§4) |
| Datagrams | `Datagrams::send(Bytes, drop)` / `recv()` | one copy each way; `Blocked` clears via `DatagramsUnblocked` |

The public API never exposes a quinn type. The copy at each boundary line is counted in §10 rather than described as zero.

---

## 3. The demultiplexer

```rust
pub struct Endpoint { /* quinn endpoint + our per-connection map */ }
impl Endpoint {
    pub fn new(udp: UdpId, config: EndpointConfig) -> Result<Endpoint, Error>;
    pub fn feed(&mut self, datagram: Datagram<'_>) -> Result<(), Error>;
    pub fn poll(&mut self) -> Option<EndpointEvent>;
    pub fn accept(&mut self) -> Option<h3::ConnId>;
    pub fn conn(&mut self, id: h3::ConnId) -> Option<&mut H3Conn>;
    pub fn output_datagram(&mut self) -> Option<OutDatagram<'_>>;
}

pub enum EndpointEvent {
    Accepted(h3::ConnId),           // a client-initiated connection exists
    Connected(h3::ConnId),
    Closed(h3::ConnId, CloseReason),
    DatagramWritable,
}
```

`h3::ConnId` is generational and distinct from the transport's `ConnId`, because one UDP socket carries many connections and none of them is a socket. That is the one place where the handle shape differs by protocol, and it is visible in the types rather than hidden behind one.

`H3Conn` exposes the same event vocabulary and send API as `conn::Conn` (`conn` §3, §5) — it is the QUIC path of the same envelope, not a second one — but it is created and owned by the endpoint, because a QUIC connection has no socket to be created from. `conn::Conn` covers the TCP path, where the transport's `ConnId` is the identity.

An h3 connection's lifecycle is reported here, not by `Io`: `poll` yields `Closed` with the same `CloseReason` vocabulary, because the proxy needs the reason to choose between a 502 and a retry and cannot infer it from a UDP socket.

A new client connection is created by the endpoint (from an accepted handle) or by `Endpoint::connect` (client, which owns the four-tuple). A server never reuses an `h3::ConnId` for a different CID.

---

## 4. Streams and control streams

A request is one client-initiated bidirectional stream: a `HEADERS` frame, optional `DATA`, optional trailing `HEADERS`. A unidirectional stream begins with a variable-length stream type:

| Type | Stream | v1 |
|---|---|---|
| 0x00 | control | required |
| 0x01 | push | never opened; received push is `H3_ID_ERROR` |
| 0x02 | QPACK encoder | opened, but carries no instructions at capacity zero |
| 0x03 | QPACK decoder | opened; receives only Section Acknowledgements if we never insert |
| other | — | ignored, and the stream is not read further |

The control stream carries `SETTINGS` first, then `GOAWAY`, `MAX_PUSH_ID` (never sent) and `CANCEL_PUSH` (never sent). A second `SETTINGS` on the same stream, or `SETTINGS` on any other stream, is `H3_FRAME_UNEXPECTED`.

The three control streams have independent output that the engine merges into connection order; a blocked request stream cannot delay them (the admission rule is `conn` §6.4, enforced here).

---

## 5. Settings

v1 sends `SETTINGS_QPACK_MAX_TABLE_CAPACITY = 0`, `SETTINGS_QPACK_BLOCKED_STREAMS = 0`, `SETTINGS_MAX_FIELD_SECTION_SIZE` (configured), and `ENABLE_CONNECT_PROTOCOL = 0`. Unknown settings are ignored; a setting appearing twice is `H3_SETTINGS_ERROR`.

Because capacity is zero, the peer must not insert entries or send encoder instructions (RFC 9204 §3.2.3), so our decoder never resolves a dynamic reference and never blocks a stream. Our encoder is literal-only. This is the deferral in the umbrella (M5) and it is the reason M3 is a framing milestone rather than a compression one.

---

## 6. QPACK in v1

- Field sections are parsed in full: `Required Insert Count`, `Base`, the representation prefixes, Huffman and the static table (99 entries). With capacity zero the required insert count is always 0 and no dynamic reference can appear; a peer that sends one is `QPACK_DECOMPRESSION_FAILED`.
- The encoder emits literal field lines (indexed-name or literal-name) with Huffman where it is shorter. No inserts, no duplicates, no dynamic references.
- The decoder stream is still read and its instructions validated, because a peer may use its right to acknowledge; an illegal instruction is `QPACK_DECODER_STREAM_ERROR`.
- The encoder stream is opened and drained; any instruction on it is a protocol error from a peer that ignored our setting.

Huffman tables and the static table are shared with h2's HPACK implementation, not duplicated.

---

## 7. Flow control and credit

QUIC's flow control is quinn's: `TransportConfig::{receive_window, stream_receive_window}`, configured to the per-stream reservation with the sum bounded like h2's. The engine reads at most one chunk ahead per stream, so the gap between "read" and "consumed" is bounded by one chunk plus the configured window.

Credit is released when `Chunks` is finalised or dropped. `Chunks` borrows the connection, so it cannot be held across an engine operation: the engine copies into a pooled chunk and finalises immediately, then turns the returned `ShouldTransmit` into output in the same `tick`. That is why `tick` runs before the output drain (`conn` §6.6).

This is the one place where the public "credit on consumption" rule (`conn` §4) is implemented at read time instead, and the bound on a slow consumer is the configured window rather than a `Body` handle. The reason is the borrow, not a preference.

---

## 8. Egress, writability and backpressure

A send-stream write returns `Blocked` at the stream or connection send limit and becomes writable again when flow-control credit arrives. The engine therefore cannot hand quinn a pre-framed connection-ordered byte stream: it pushes frames into the transport and reports `Event::StreamWritable` when the transport says a stream can take more.

`Endpoint::output_datagram` only drains `poll_transmit`. The application sends the returned datagrams on the `UdpId`; the engine never writes a socket.

Pushing everything into quinn instead would make quinn's send buffer the real bound and blow the memory budget, so a blocked stream's bytes wait in the engine's bounded per-stream queue.

---

## 9. GOAWAY, shutdown, CONNECT

`Conn::finish` requests a drain: the engine sends `GOAWAY` with a stream id, stops accepting new requests above it, lets in-flight ones finish, and reports `Drained` when the last is terminal (`conn` §6.5). A `Drain` deadline bounds it.

A peer `GOAWAY` is `Event::GoAway`; requests above its id were rejected and are safe to retry. The `H3_REQUEST_REJECTED` code is used only for admission refusal, never for a stream that ran.

Plain `CONNECT` sets `:method = CONNECT` and `:protocol` absent; extended CONNECT is not advertised. The stream becomes a `Tunnel`, and its half-close is `END_STREAM` on that stream.

---

## 10. Datagrams (RFC 9221)

`Conn::send_datagram(bytes)` maps to `Datagrams::send`. `max_size()` is the peer's limit, capped by path MTU; a send above it is `Error::DatagramTooLarge`, never a silent drop. `Blocked` is surfaced as `Event::DatagramWritable`, cleared by `DatagramsUnblocked`, so a full send buffer is a wait rather than a busy loop.

Received datagrams surface as `Event::Datagram` on the connection. They carry no ECN and no per-datagram address: a datagram frame can be coalesced into a packet with other frames and quinn reports no packet metadata. The public type says `None` rather than inventing a value.

Copy budget at this boundary: one per inbound packet, one per inbound body byte, one per outbound transmit batch, one per application datagram in each direction.

---

## 11. Errors

| Condition | Code |
|---|---|
| Malformed frame, wrong stream type, duplicate SETTINGS | `H3_FRAME_UNEXPECTED` / `H3_FRAME_ERROR` |
| QPACK field-section failure | `QPACK_DECOMPRESSION_FAILED` |
| QPACK decoder-stream failure | `QPACK_DECODER_STREAM_ERROR` |
| Request above GOAWAY, or refused at admission | `H3_REQUEST_REJECTED` |
| Dispatched stream cancelled or timed out | `H3_REQUEST_CANCELLED` |
| Stream body cut before END_STREAM | `H3_REQUEST_INCOMPLETE` |
| Exceeding configured limits | `H3_EXCESSIVE_LOAD` |

Connection errors close the QUIC connection; stream errors reset one stream. The same "refused versus may have run" distinction as h2 applies, and the codes differ for the same reason.

---

## 12. What a builder decides

`max_field_section_size`, the per-stream queue depth, when to coalesce `DATA` into one stream write, and whether to use `poll_transmit`'s GSO batching.

Not theirs: the capacity-zero QPACK cut, the control-stream types and their ordering, the demultiplexer's ownership of connection lifecycle, the read-time credit and its reason, `StreamWritable`, and the datagram `Blocked` path.
