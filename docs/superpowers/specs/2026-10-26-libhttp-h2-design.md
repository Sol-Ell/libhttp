# libhttp h2 — Design

**Status:** subsystem design, rewritten after verification. Owns HTTP/2 framing, HPACK, stream state, flow control and the settings handshake.
**Parent:** `2026-10-26-libhttp-design.md`.
**Consumes:** `conn` (`Event`, `Head`, `Body`, `StreamId`, `StreamKind`, `Flow`, `OpenError`, `codes`, `send_*`), transport (`Chunk`, `CreditSink`).
**Consumed by:** `proxy`.

---

## 1. Scope

**Owns:** frames and their legality, HPACK, connection and stream flow control, stream admission and concurrency, `HEADERS`/`DATA`/`RST_STREAM`/`SETTINGS`/`GOAWAY`/`WINDOW_UPDATE`/`PING`/`CONTINUATION`, trailers, `CONNECT`, and the h2c preface.

**Does not own:** TLS and ALPN (`tls`), the event vocabulary (`conn`), or push and priority, which are out of scope (§2).

---

## 2. Out of scope, with the exact rule

| Feature | Rule |
|---|---|
| Push | A client sends `SETTINGS_ENABLE_PUSH = 0`, which is the normal request and must be honoured. A **server** must not send that setting, and a client that receives it — or receives any value other than 0 — treats it as `PROTOCOL_ERROR` (RFC 9113 §6.5.2). A `PUSH_PROMISE` is `PROTOCOL_ERROR`, but only once our 0 has been acknowledged: before the peer has seen the setting it may legally push, and killing the connection on that race is a bug |
| Priority | `PRIORITY` frames and `HEADERS` priority fields are parsed and discarded; the frame is legal in every stream state |
| Extended CONNECT | not advertised; the request is refused |
| `101` | impossible |

---

## 3. Framing

Fixed 9-byte header; frames larger than our `SETTINGS_MAX_FRAME_SIZE` are `FRAME_SIZE_ERROR`. Unknown frame types are **ignored** (RFC 9113 §4.1), not rejected.

The connection preface is the 24-byte client string; `conn`'s `Detect` committed to h2 before this engine saw a byte.

`SETTINGS` must be the first frame from each peer and is acknowledged with a `SETTINGS` ACK. A missing or misplaced `SETTINGS` is `PROTOCOL_ERROR`.

`HEADERS` may be continued by `CONTINUATION`, and **nothing may interleave** between the first `HEADERS` and its final `CONTINUATION`; any other frame in that window is `PROTOCOL_ERROR`. That rule is a fuzz target on its own.

### 3.1 Stream states

| State | Legal inbound frames |
|---|---|
| idle | `HEADERS`, `PRIORITY`; anything else `PROTOCOL_ERROR` |
| open | all |
| half-closed (remote) | everything except `DATA`/`HEADERS`; those are `STREAM_CLOSED` |
| half-closed (local) | all — we closed our send side, not our receive side |
| closed | `PRIORITY`; `WINDOW_UPDATE` and `RST_STREAM` only in the short window after the transition; everything else is **minimally processed** (HPACK decoded), never skipped. `DATA` is a stream error — `RST_STREAM` is barred there — and it still counts toward the connection flow-control window (RFC 9113 §5.1) |

"Minimally processed" is not "ignored": a `HEADERS` on a stream we just reset must still be HPACK-decoded, because the dynamic table is connection state and skipping one block desynchronises every later one. That obligation applies to refused streams too (§4).

---

## 4. Streams and admission

Odd ids for client-initiated, even for server-initiated; a new id greater than the last from that peer, or `PROTOCOL_ERROR`. A closed id cannot be reopened.

**Admission.** We advertise `SETTINGS_MAX_CONCURRENT_STREAMS = reservation_cap / min_stream_reservation`, floored at one maximum frame per stream, so the limit and the cap cannot drift. A `HEADERS` that would exceed it is refused with `REFUSED_STREAM` before dispatch, which is the only place that code is legal.

**Refusal still decodes the block.** The refused stream's `HEADERS` (and any trailer `HEADERS` that arrives before we reset) are decoded and discarded; otherwise the HPACK table diverges and the next accepted request fails with `COMPRESSION_ERROR`.

**Outbound admission.** `open` returns `OpenError::WouldBlock` at the peer's `SETTINGS_MAX_CONCURRENT_STREAMS`, and the engine emits `Event::StreamsAvailable` when a slot frees. `StreamWritable` is emitted when a stream's send window reopens. Without both, the relay waits forever (`proxy` §4).

`open(StreamKind::Connect)` is the CONNECT path (§10).

---

## 5. HPACK

Implemented here: static table, Huffman, the dynamic table with eviction, and both instruction sets.

- The decoder advertises `SETTINGS_HEADER_TABLE_SIZE` (default 4096) and evicts to match. An encoder reference to an evicted entry is `COMPRESSION_ERROR`, which is a **connection** error because the table is unrecoverable.
- The encoder keeps a table bounded by the peer's advertised size. h2 header blocks arrive in order on the connection, so eviction needs no per-block reference counting — unlike QPACK, whose out-of-order streams are why its table is deferred to M5.
- Sensitive values use the never-indexed representation.

Header blocks decode into a `Head` whose arena outlives the frames.

---

## 6. Flow control

Two windows per direction, and they are **not** the same kind of thing:

- The **connection** window starts at 65535 and is changed only by `WINDOW_UPDATE`. `SETTINGS_INITIAL_WINDOW_SIZE` does **not** affect it.
- Each **stream** window starts at the peer's `SETTINGS_INITIAL_WINDOW_SIZE` (65535 by default); a change to that setting retroactively shifts every open stream's window by the delta, which is why a stream window can go **negative** and why the engine must track it as a signed value (RFC 9113 §6.9.2).
- Neither window may exceed 2^31 − 1; an overflow is `FLOW_CONTROL_ERROR`.

Credit policy, which decides whether one slow consumer stalls its neighbours:

- Connection credit returns as soon as DATA is admitted into the bounded per-stream reservation.
- Stream credit returns when the application consumes the `Body` (`conn` §4), via `CreditSink`, drained by `Conn::tick`.
- Per-stream reservations sum to a hard cap with a floor of one maximum frame.
- A stream that cannot drain within its reservation is reset with `CANCEL`.

`WINDOW_UPDATE` is emitted from `tick`, which is why the pump calls `tick` before draining output (`conn` §6.6).

---

## 7. Headers

Pseudo-headers precede regular fields; `:method`, `:scheme`, `:path` for a request, `:authority` optional, `:status` for a response, none in trailers. Duplicate or misplaced pseudo-headers, an undefined one, or a non-three-digit `:status` is `PROTOCOL_ERROR`.

**The one exemption:** `CONNECT` has `:method` and `:authority` only — no `:scheme`, no `:path` (RFC 9113 §8.5). The exemption lives here, not only in §10, because the general rule would otherwise reject it.

Field validation: uppercase names, leading or trailing whitespace, NUL/CR/LF, and the connection-specific fields (`connection`, `keep-alive`, `proxy-connection`, `transfer-encoding`, `upgrade`) are malformed. `te` is allowed only with `trailers`.

`Host` versus `:authority`: a disagreement is **malformed** (RFC 9113 §8.3.1 says a server SHOULD reject it), and this engine does. There is no "reconciled per protocol" rule; the relay rejects it too.

`content-length`, when present, must equal the sum of `DATA`, or `PROTOCOL_ERROR`.

---

## 8. GOAWAY and graceful shutdown

Two-phase, the same shape as h3. `Conn::stop_accepting` sends GOAWAY with `last_stream = 2^31 − 1` — stop opening, reject nothing. `Conn::finish` requests the drain; the final GOAWAY with the last accepted id is emitted only when every accepted stream is terminal or the `Drain` deadline fires.

`Drained` follows `conn` §6.5's three conditions, which this document does not restate. A GOAWAY that overtakes a response body truncates the streams it acknowledges; that is the trap the two-phase form exists to avoid.

A peer GOAWAY is `Event::GoAway`; streams above `last_stream` never ran and are retryable (`proxy` §6). A GOAWAY whose `last_stream` is the maximum id asks us to stop opening without rejecting anything: that is `Event::StopAccepting`, not an error, and it is the signal the relay's no-new-streams rule consumes.

---

## 9. Errors, with fatality

| Code | Level | Condition |
|---|---|---|
| `PROTOCOL_ERROR` | connection | malformed frame, pseudo-header violation, interleaved `CONTINUATION`, preface/`SETTINGS` rules |
| `COMPRESSION_ERROR` | connection | any HPACK failure — the table is unrecoverable |
| `FLOW_CONTROL_ERROR` | connection | window overrun |
| `FRAME_SIZE_ERROR` | connection | frame above `SETTINGS_MAX_FRAME_SIZE` |
| `REFUSED_STREAM` | stream | admission refusal only: nothing was processed |
| `CANCEL` | stream | a dispatched stream timed out, or could not drain |
| `STREAM_CLOSED` | stream | `DATA`/`HEADERS` on a non-open stream |
| `CONNECT_ERROR` | stream | a CONNECT that failed to establish |
| `ENHANCE_YOUR_CALM` | either | a peer exceeding configured limits without breaking framing |

`REFUSED_STREAM` promises the peer nothing ran. A deadline or a drain failure must never carry it.

---

## 10. CONNECT

`open(StreamKind::Connect)`; `:method = CONNECT`, `:authority` required, no `:scheme`/`:path`. On success the stream becomes a `Tunnel`, and half-close is `END_STREAM` on that stream — the connection is untouched, which is why the tunnel API is per stream. A failed attempt is `CONNECT_ERROR`.

---

## 11. Output admission

A stream's pending body is admitted only when its window covers it; control frames (`WINDOW_UPDATE`, `PING` ACK, `SETTINGS` ACK, `RST_STREAM`, `GOAWAY`) are never gated. A blocked stream's data cannot delay a PING ACK or a GOAWAY (`conn` §6.4).

---

## 12. What a builder decides

`SETTINGS` values beyond the contract ones, HPACK eviction heuristics, and whether `DATA` frames are coalesced.

Not theirs: admission and the refusal code, the decode-refused-blocks obligation, the credit policy, the state table, the pseudo-header rules and the CONNECT exemption, the negative-window tracking, and the two-phase GOAWAY.
