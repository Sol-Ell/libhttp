# libhttp h2 — Design

**Status:** subsystem design. Owns HTTP/2 framing, HPACK, stream state, flow control and the settings handshake.
**Parent:** `2026-10-26-libhttp-design.md`.
**Consumes:** `conn` (`Event`, `Head`, `Body`, `StreamId`, `Flow`, `send_*`), transport (`Chunk`, `CreditSink`).
**Consumed by:** `proxy`.

---

## 1. Scope

**Owns:** frames and the frame state machine, HPACK encoding and decoding, connection and stream flow control, stream admission and concurrency, `HEADERS`/`DATA`/`RST_STREAM`/`SETTINGS`/`GOAWAY`/`WINDOW_UPDATE`/`PING`/`CONTINUATION`, trailers, `CONNECT`, and the h2c connection preface.

**Does not own:** TLS and ALPN (`tls`), the event vocabulary (`conn`), or push and priority, which are out of scope for v1 (§2).

---

## 2. Out of scope, and what happens when a peer asks

| Feature | v1 |
|---|---|
| Server push | `PUSH_PROMISE` from a server is a connection error `PROTOCOL_ERROR`; `SETTINGS_ENABLE_PUSH` is sent as 0 and a peer that sends 1 is a `PROTOCOL_ERROR` |
| Priority | `HEADERS` priority fields are parsed and discarded; `PRIORITY` frames are parsed and discarded; no scheduling changes |
| Extended CONNECT (RFC 8441) | not advertised; a request that needs it is refused |
| `101` | impossible on h2 |

Parsing-and-discarding priority is deliberate: a frame that is legal to receive must not kill the connection, and rejecting priority would break peers that send it unconditionally.

---

## 3. Framing

The fixed 9-byte frame header and the v1 frame set: `DATA`, `HEADERS`, `PRIORITY`, `RST_STREAM`, `SETTINGS`, `PUSH_PROMISE`, `PING`, `GOAWAY`, `WINDOW_UPDATE`, `CONTINUATION`, plus unknown types, which are **ignored** (RFC 9113 §4.1) rather than rejected.

`SETTINGS_MAX_FRAME_SIZE` is enforced: a frame larger than the advertised maximum is `FRAME_SIZE_ERROR`. A `HEADERS` block that does not fit one frame is continued with `CONTINUATION`, and **nothing may interleave** between the first `HEADERS` and the final `CONTINUATION` — any other frame in that window is a connection error `PROTOCOL_ERROR`. That rule is the one most likely to be got wrong, so it is a fuzz target on its own.

`h2c` uses prior knowledge: the connection begins with the 24-byte preface, and `conn`'s `Detect` already committed to h2 before this engine sees a byte. There is no `Upgrade` dance.

---

## 4. Streams

Stream ids are odd for client-initiated and even for server-initiated; a new id must be greater than the last seen from that peer, or the connection is `PROTOCOL_ERROR`. A stream that is closed cannot be reopened; a frame for a closed stream is `STREAM_CLOSED` (or ignored for `RST_STREAM` and `WINDOW_UPDATE`, per RFC 9113 §5.1).

**Admission.** The advertised `SETTINGS_MAX_CONCURRENT_STREAMS` is derived, not guessed: `reservation_cap / min_stream_reservation`, with a floor of one maximum frame per stream, so the advertised limit and the reservation cap cannot drift apart. A `HEADERS` that would exceed it is refused with `REFUSED_STREAM` — the code that promises *not processed* — before any work is done. That code is only legal here because the engine refuses at the frame, before dispatch; a stream that has been dispatched and later times out carries `CANCEL` instead (§9).

A stream counts as concurrent from `HEADERS` until it is closed, including half-closed states.

---

## 5. HPACK

Implemented here, not delegated: static table, Huffman coding, dynamic table with eviction, and the encoder/decoder instruction set.

- The decoder advertises `SETTINGS_HEADER_TABLE_SIZE` (default 4096) and evicts to match; an encoder that references a needed entry after it was evicted is `COMPRESSION_ERROR`, which is a **connection** error because the decoder's state is now unrecoverable.
- The encoder keeps a dynamic table bounded by the peer's advertised size. Unlike QPACK, h2 header blocks arrive in order on the connection, so eviction needs no per-block reference counting: an eviction is simply an instruction the decoder applies in order. That asymmetry with QPACK is the reason QPACK's dynamic table is deferred to M5 and this one is not.
- Sensitive values (authorization, cookie) use the never-indexed representation; the choice is per field and lives in the serializer.

Header blocks are decoded into a `Head` (`conn` §2) with the arena owned by the block, so a `Head` outlives the frames it arrived in.

---

## 6. Flow control

Two windows per direction: connection and stream. Initial values are 65535 or `SETTINGS_INITIAL_WINDOW_SIZE`; a `DATA` that exceeds either is `FLOW_CONTROL_ERROR`.

Credit policy, which is the part that decides whether one slow consumer stalls its neighbours:

- **Connection window**: returned as soon as DATA is admitted into the connection's bounded per-stream reservation, not when the application consumes it.
- **Stream window**: returned when the application consumes the `Body` handle (conn §4), via the transport's `CreditSink`, drained by `Conn::tick` (`conn` §6.6).
- The sum of per-stream reservations is a hard cap, and each stream has a floor of at least one maximum frame, so `cap/N` cannot collapse to zero while a stream is legitimate.
- A stream that cannot drain within its reservation is reset with `CANCEL` — see §9 for why not `REFUSED_STREAM`.

`WINDOW_UPDATE` is emitted from `tick`, which is why the pump calls `tick` before draining output (`conn` §6.6).

---

## 7. Headers

Pseudo-headers must precede regular fields; `:method`, `:scheme` and `:path` are required for a request (`:path` may be `*` only for `OPTIONS`), `:authority` is optional, and a response carries `:status`. A duplicate pseudo-header, a pseudo-header after a regular field, an undefined pseudo-header, or a `:status` that is not three digits is `PROTOCOL_ERROR`.

Connection-specific fields are forbidden: `connection`, `keep-alive`, `proxy-connection`, `transfer-encoding`, `upgrade` — any of them is a malformed request. `te` is allowed only with the value `trailers`.

`content-length`, when present, must equal the sum of `DATA` payloads, or the stream is `PROTOCOL_ERROR`. `Host` is not required; `:authority` carries it, and a mismatched `host` is not an error but is preserved for the relay.

Trailers are a `HEADERS` block after the body, ending the stream (`conn` §3).

---

## 8. GOAWAY and graceful shutdown

`finish` does not send GOAWAY immediately. The engine sends a first GOAWAY with `last_stream = 2^31 - 1` only when the application asks to stop accepting new streams, then a second with the last accepted id once the in-flight streams are terminal or the `Drain` deadline fires.

`Conn::finish` (conn §6.5) is the drain request; `Drained` is emitted when the last stream is terminal and no output is eligible. A GOAWAY that overtakes a response body would truncate the very streams it acknowledges — that trap is stated in `conn` and is the reason the two-phase form is required here.

A peer GOAWAY is an `Event::GoAway`; streams above `last_stream` were not processed and are safe to retry, and the engine does not silently retry them.

---

## 9. Stream errors versus connection errors

| Code | Use |
|---|---|
| `REFUSED_STREAM` | admission refusal only: nothing was processed |
| `CANCEL` | a dispatched stream that timed out, or a consumer that could not drain within its reservation |
| `STREAM_CLOSED` | a frame on a closed stream where RFC 9113 requires it |
| `PROTOCOL_ERROR` | malformed frames, pseudo-header violations, interleaved `CONTINUATION` |
| `COMPRESSION_ERROR` | an HPACK failure; always connection-level |
| `FLOW_CONTROL_ERROR` | a window violation |
| `ENHANCE_YOUR_CALM` | a peer exceeding configured limits without violating a framing rule |

The distinction that matters: `REFUSED_STREAM` promises the peer nothing ran. A deadline or a drain failure must never carry it, because a retrying peer would replay side effects.

---

## 10. CONNECT

Plain `CONNECT` is supported: `:method = CONNECT`, `:authority` required, no `:scheme` or `:path`. On success the stream becomes a `Tunnel` (`conn` §3), and its half-close is `END_STREAM` on that stream — the connection is untouched, which is why the tunnel API is per-stream.

Extended CONNECT is not advertised and cannot be used.

---

## 11. Output admission

Per-stream pending bodies are admitted only when that stream's window covers them; control frames (`WINDOW_UPDATE`, `PING` ACK, `SETTINGS` ACK, `RST_STREAM`, `GOAWAY`) are never gated by a stream. A blocked stream's data therefore cannot delay a PING ACK or a GOAWAY — the rule is `conn` §6.4 and is enforced here at the point frames are queued.

---

## 12. What a builder decides

`SETTINGS` values beyond the ones that are contracts (initial window, max frame size, max concurrent derived from the cap), HPACK's dynamic table eviction heuristic, and whether `DATA` is coalesced into larger frames.

Not theirs: the admission derivation and its refusal code, the credit policy, the interleaved-`CONTINUATION` rule, the pseudo-header rules, the forbidden fields, and the two-phase GOAWAY.
