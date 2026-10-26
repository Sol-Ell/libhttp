# libhttp proxy — Design

**Status:** subsystem design. The relay, and the proof that the public API is sufficient.
**Parent:** `2026-10-26-libhttp-design.md`.
**Consumes:** `conn` and every protocol engine, plus `dispatch` only for upstream name resolution.

---

## 1. Scope

**Owns:** the relay state machine, hop-by-hop handling, body and credit relay, tunnel forwarding, failure mapping, and the rule that makes cross-protocol relaying safe.

**Does not own:** parsing, framing, flow control or TLS. It touches nothing private: if this document ever needs a call the public API does not have, the API is wrong and gets fixed (`conn`, `proxy`).

**Shape:** a state machine on the pump thread, built from `Conn` objects and their events. It never blocks and never runs a worker. Its only worker involvement is name resolution before an upstream connect (`Server`, `Client`).

---

## 2. Two leg classes

The relay is not uniform across protocols, and pretending otherwise is the failure this document exists to prevent.

- **h2 and h3 legs are stream-granular.** Each stream has its own window, its own `Body`, and its own refusal. One slow consumer stalls one stream.
- **An h1 leg is connection-granular.** There is no per-stream window, no per-stream abort, and stopping reads stops the socket. So the relay **never multiplexes concurrent streaming exchanges onto one h1 connection**: an h1 leg carries one streaming exchange at a time, and the connection is pooled only for head-complete exchanges.

The consequence is stated rather than hidden: an h1 leg is a bottleneck by protocol, not by implementation, and the relay's pooling policy is what keeps it from stalling unrelated traffic.

---

## 3. Pairs

All six: h1↔h1, h1↔h2, h2↔h2, h1↔h3, h2↔h3, h3↔h3. The only per-protocol code is connection setup and the leg class above; heads, bodies, trailers, resets and GOAWAY move through the shared vocabulary (`conn` §3).

---

## 4. A request

1. Read `Event::Head` from the downstream leg.
2. Strip hop-by-hop fields from the typed list: `connection` and every token it names, `keep-alive`, `proxy-connection`, `transfer-encoding`, `upgrade`, `te` (unless `trailers`). `host` and `:authority` are reconciled per protocol.
3. Decide the trailer policy **before** writing the head upstream: if the downstream may produce trailers, set `PreferChunked` on an h1 upstream and drop a source `content-length` (`h1` §4).
4. `open` an upstream stream, retrying on `WouldBlock` after `StreamsAvailable`.
5. Relay the body as `Body` handles. `send_data` returning `Flow::Blocked(body)` means the upstream reservation is full: keep the handle in the stream's outbox and retry on `StreamWritable`, never drop it.
6. Relay trailers or `End` as the source ended. The two are different messages and are not collapsed.

The downstream leg is not read further while a stream's body is blocked upstream — but only that stream: on a stream-granular leg the connection keeps flowing for its other streams, and on an h1 leg there are no other streams.

---

## 5. A response

The mirror image, with two additions:

- A response head that arrives with a `content-length` while the source may still send trailers is re-framed (`h1` §4) rather than trusted.
- `1xx` responses other than `101` are forwarded as ordinary heads and do not end the exchange.

---

## 6. Failure mapping

| Failure | Relay action |
|---|---|
| Upstream connect fails | `502` if no response head has been sent; otherwise close the downstream stream |
| Upstream `ConnectFailed(TimedOut)` or a fired deadline | `504`, same rule |
| Upstream `Reset` before a head | `502` |
| Upstream `Reset` after a head | propagate `Reset`; the body is already streaming |
| Upstream `GoAway` | stop opening new streams above `last_stream`; retry those with another connection, **only** those |
| Downstream disappears | cancel the upstream stream with the may-have-run code (`dispatch` §3.5) |
| h1 leg fails mid-stream | close the leg; every stream on it gets a gateway error, because h1 cannot abort one response |

The `last_stream` retry rule is the one that must not be generalized: streams above it were rejected and are safe to replay; streams at or below it may have run.

---

## 7. Tunnels

`CONNECT` and `101` become `Tunnel` pairs. Half-close is `end_tunnel` on every leg, because the API is per stream: h1 maps its single tunnel to the connection half-close, while h2 and h3 use `END_STREAM` and leave the connection alone. The relay forwards it in both directions.

On an h1-over-TLS leg the half-close is `close_notify` plus `shutdown(SHUT_WR)`, and reading continues; the matrix is `tls` §3.3, not restated here.

A tunnel closes when both directions have ended, when either side resets, or when the `HalfClose` deadline fires.

---

## 8. Flow control across legs

The relay never overruns a downstream reservation, and never reads a source it cannot write. Because the source's credit travels on the `Body` (`conn` §4), the relay's own buffers are bounded by what it has accepted and not yet delivered, per stream — not by a global queue.

On an h1 leg, "per stream" is "per connection", which is why §2 forbids multiplexing there.

---

## 9. Topology

v1 is single-driver-thread per proxy instance. Multi-shard relaying uses the same primitives — a `Body` can cross a shard, and the pool's batched return plus `wake_pump` handle the wakeup — but it is not in the gate and is not required for correctness.

---

## 10. What a builder decides

Connection pooling policy per leg class (keep-alive vs one-shot), upstream selection and load balancing, and the retry budget for `GoAway`-rejected streams.

Not theirs: the two leg classes, the hop-by-hop list, the trailer-policy-before-head rule, `Flow::Blocked` handling, the failure table, and the `last_stream` retry rule.
