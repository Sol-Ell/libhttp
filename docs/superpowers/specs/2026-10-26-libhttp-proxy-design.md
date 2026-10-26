# libhttp proxy — Design

**Status:** subsystem design, rewritten after verification. The relay, and the proof that the public API is sufficient.
**Parent:** `2026-10-26-libhttp-design.md`.
**Consumes:** `conn` (`Conn`, `Event`, `Body`, `Flow`, `StreamId`, `StreamKind`, `Head`, `HeadBuilder`, `strip_hop_by_hop`, `TrailerPolicy`, `codes`, `ConnError`, `OpenError`), transport (`CloseReason`, `HalfClose`), `tls`, `dispatch` (name resolution only).

---

## 1. Scope

**Owns:** the relay state machine, protocol translation, hop-by-hop handling, body and credit relay, tunnel forwarding, failure mapping and retry policy.

**Does not own:** parsing, framing, flow control, TLS. It uses only the public API; if it needs a call that does not exist, the API is wrong and gets fixed.

**Shape:** a state machine on the pump thread. It never blocks and never runs a worker; its only worker involvement is upstream name resolution (`conn`/`Server`).

---

## 2. Leg classes

The relay is not uniform, and pretending otherwise is the failure this document exists to prevent.

- **h2 and h3 legs are stream-granular.** Each stream has its own window, its own `Body`, and its own refusal; one slow consumer stalls one stream.
- **An h1 leg is connection-granular.** No per-stream window, no per-stream abort, stopping reads stops the socket. The relay never multiplexes concurrent *streaming* exchanges onto one h1 connection.

"Head-complete" means the exchange's body is fully buffered downstream (a `Content-Length` under the buffering threshold, or no body). Those may share an h1 connection sequentially. A streaming exchange gets its own h1 connection for its lifetime, because there is no other way to bound it.

---

## 3. Pairs and staging

All six pairs exist in the API, but they land by milestone: **M1** h1↔h1; **M2** h1↔h2, h2↔h2; **M3** h1↔h3, h2↔h3, h3↔h3. The h3 pairs are last because they depend on `h3::Endpoint`, and the gate for each milestone names the pairs it covers.

The only per-protocol code is connection setup and the leg class; heads, bodies, trailers, resets and GOAWAY move through the shared vocabulary.

---

## 4. A request

1. Read `Event::Head` from the downstream leg. Build a new head with `HeadBuilder`, because `Fields` is read-only (`conn` §2): `strip_hop_by_hop(&mut builder, view.fields())`.
2. Translate the target and scheme: h1's `Target::Origin` plus `Host` becomes h2/h3 `:path` plus `:authority`; the reverse fills `Host` from `:authority`. h1's `Target::Absolute` is reduced to origin-form on h2/h3. A mismatch between `Host` and `:authority` is not "reconciled" — RFC 9113 §8.3.1 makes it malformed, and this relay rejects it.
3. Decide the trailer policy **before** writing the head: if the source may produce trailers, set `TrailerPolicy::PreferChunked` on an h1 upstream and drop a source `content-length`.
4. `open(StreamKind::Request)` upstream; `OpenError::WouldBlock` means wait for `Event::StreamsAvailable`, `Refused` means the peer will not take it.
5. Relay the body as `Body` handles. `Flow::Blocked(body)` means the upstream reservation is full: keep the handle in the stream's outbox and retry on `StreamWritable`. Dropping it would be a lost body.
6. Relay `End` or `Trailers` as the source ended.

The upstream is not asked for more while that stream's body is blocked; other streams on a stream-granular leg keep flowing, and an h1 leg has no others by §2.

---

## 5. A response

The mirror image, with three additions:

- A response head carrying `content-length` while the source may still send trailers is re-framed rather than trusted (`h1` §4).
- Interim `1xx` heads are forwarded as heads and do not end the exchange. `101` is special: its `Connection` and `Upgrade` fields are forwarded rather than stripped, because RFC 9110 §7.6.1 requires them and the §4 rule would delete exactly the fields that make the upgrade work. `101` then becomes a `Tunnel`.
- A `2xx` to `CONNECT` has no body and must not be framed as one.

---

## 6. Failure mapping and retry

Codes come from `conn::codes`; the relay never hardcodes `0x8` or `0x10c`.

| Upstream outcome | Before a response head | After a response head |
|---|---|---|
| `Reset(REFUSED_STREAM)` / `Reset(H3_REQUEST_REJECTED)` | **not an error**: the stream never ran, so retry on another connection, bounded by the retry budget | impossible: no head was sent |
| `Reset(CANCEL)` / `Reset(H3_REQUEST_CANCELLED)` | do not retry; `502` | propagate the reset |
| `Event::GoAway` above `last_stream` | those streams never ran: retry them on another connection | n/a |
| `Event::StopAccepting` (initial GOAWAY, max id) | stop opening **new** streams on that connection; keep using the ones already open | same |
| `ConnError::Connect(..)` / `ConnectFailed(TimedOut)` | `502` / `504` | `502`/`504` |
| `Closed(PeerReset)` before any head | `502` | propagate |
| `Closed(Local)` (downstream gone) | cancel upstream with `CANCEL` / `H3_REQUEST_CANCELLED` | same |
| h1 leg failure | close the leg; the exchange on it gets `502` | same |

Two distinctions are load-bearing and are not interchangeable:

- **Refused** means nothing ran. Retrying is safe, and returning `502` would be a lie that also breaks idempotency assumptions downstream.
- **Stop accepting** is not an error. It means new streams must go elsewhere while existing ones finish; a relay that keeps opening streams on that connection ignores the only signal the peer has.

`CloseReason` does not encode retryability: only `PeerReset` and `Peer` are unambiguous, and the refusal codes arrive as `Reset`, never as a close. The table is therefore keyed on the event, not on the close reason.

---

## 7. Tunnels

`CONNECT` is opened with `StreamKind::Connect`. On success both legs emit `Tunnel`, and bytes move through `read_tunnel`/`write_tunnel` (`conn` §5). Half-close is `end_tunnel` on every leg — per stream on h2/h3, and the connection half-close on h1, where the tunnel is the only stream.

A tunnel closes when both directions end, either side resets, or the `HalfClose` deadline fires.

---

## 8. Flow control across legs

Per leg, not globally:

- **h2 downstream:** credit travels on the `Body`; the relay holds at most one stream's unrelayed bodies, and the reservation bounds it.
- **h3 downstream:** there is no `Body` credit (`conn` §4: `sink` is `None`, read-time release). The bound is the transport's configured receive window, so the relay's own buffer is bounded by one chunk per stream plus what the upstream outbox holds.
- **h1 downstream:** connection-granular, and §2 is what keeps that from stalling unrelated traffic.

The relay never reads a source it cannot write, and never overruns a destination reservation.

---

## 9. Draining

`stop_accepting` on a leg when shutdown begins; `finish` when it must close gracefully. A leg is drained when `Conn::poll` yields `Drained` (`conn` §6.5) and the relay has no outbox for it; only then is `drv.shutdown_write` legal (transport §4.5). The relay drains both legs of an exchange together, so a half-closed pair cannot hold a socket open past the `Drain` deadline.

---

## 10. Topology

v1 is single-driver-thread per instance. Multi-shard relaying uses the same primitives — a `Body` crosses shards, the pool batches returns, `wake_pump` wakes the target — but is not in the gate.

---

## 11. What a builder decides

Connection pooling per leg class, upstream selection, the retry budget, and the buffering threshold that separates "head-complete" from "streaming".

Not theirs: the leg classes, the hop-by-hop list and its `101` exception, the trailer-policy-before-head rule, `Flow::Blocked` handling, the failure table, and the refused-versus-may-have-run distinction.
