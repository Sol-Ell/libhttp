# libhttp h1 — Design

**Status:** subsystem design, rewritten after verification. Owns HTTP/1.1 parsing, serialization, framing, pipeline ordering and tunnels.
**Parent:** `2026-10-26-libhttp-design.md`.
**Consumes:** `conn` (`Head`, `HeadBuilder`, `Fields`, `Event`, `StreamId`, `TrailerPolicy`, `send_*`, `body_wanted`), transport (`Chunk`, `ChunkMut`).
**Consumed by:** `proxy`.

---

## 1. Scope

**Owns:** request and status lines, field parsing, framing (content-length, chunked, close-delimited), trailer sections, `100-continue`, keep-alive and reuse, pipeline ordering, `CONNECT` and `101`, and the parser's rejection rules — for both the server and the client role.

**Does not own:** the event vocabulary (`conn` §3), body handles (`conn` §4), TLS, or the clock. It arms deadlines only through `Conn::arm` (`conn` §6.6); it never calls a driver.

---

## 2. Streams and ordering

HTTP/1.1 has no stream ids; the engine assigns **synthetic ids in pipeline order**: 0, 1, 2, …, one per request, never reused. On a client connection the engine assigns the same way, so the application never chooses an h1 id.

Responses must be emitted in ascending id order. `send_head` for a later id while an earlier stream is open returns `ConnError::StreamDone`; the engine does not queue it, because a queue would be an unbounded buffer the caller cannot see.

At most one request body is *open* at a time. A later pipelined request may be parsed, but its body is not admitted until the earlier stream is terminal. That is the h1 substitute for a stream window, and it is why an h1 leg cannot multiplex streaming exchanges (`proxy` §2).

---

## 3. Parsing

### 3.1 Lines and the head

`method SP request-target SP HTTP-version CRLF`, one space between fields, none before the CR. The status line is `HTTP-version SP status-code SP reason-phrase CRLF`; the reason phrase is validated and discarded, regenerated on output.

Rejected, not repaired:

- bare `CR` or `LF`, NUL, a line over the maximum;
- `obs-fold` (a continuation line starting with SP/HTAB);
- **whitespace or an empty line between the request line and the first field** (RFC 9112 §2.2);
- whitespace before a field colon — the `Transfer-Encoding : chunked` smuggling vector;
- a field name that is not a token, a value containing a CTL other than HTAB;
- limits: head size, field count, chunk size, pipeline depth.

### 3.2 Request framing

In precedence order:

1. `Transfer-Encoding` present. If its **final** coding is not `chunked`, the request is `400` and the connection is closed — RFC 9112 §6.1 makes this a MUST, not a `501`. If `chunked` appears **more than once**, `400`: applying chunked twice is the second-most-used smuggling primitive after CL/TE. If the final coding is `chunked` and some earlier coding is unsupported, `501` and close.
2. `Content-Length`: a single bare decimal. Duplicates, a comma list, a sign, or a value longer than 19 digits are `400`.
3. Both present → `400`, always, regardless of values.
4. Neither → no body.

### 3.3 Response framing

`Transfer-Encoding` wins as above, but an unsupported coding on a response is **close-delimited**, not `501` — there is no request to reject. `Content-Length` next. Otherwise the body is close-delimited and the connection cannot be reused.

Chunked: hex size, optional `;` extensions parsed and discarded, size ≤ the configured maximum, zero chunk, trailer section, `CRLF`.

### 3.4 What the parser rejects

The list in §3.1 plus: duplicate or missing `Host` on an HTTP/1.1 request; a trailer field that is forbidden; a missing final `CRLF`; the HTTP/2 preface reaching this engine (a `Detect` bug). Each is a fuzz target.

### 3.5 Undrained bodies

Whether a body was consumed is not a parser question. The keep-alive decision owns it: a connection may be reused only when every request on it reached its terminal byte **and** every response body was fully written. If the application ends a response with its request body unread, the engine writes `Connection: close` and closes after the response. It never drains an attacker-chosen length into the void.

---

## 4. Serialization

Order: status line, fields, empty line, body. `Connection: close` is added when the engine decided not to reuse; otherwise omitted. `Transfer-Encoding` is written only when the engine chose chunked.

**Body framing is decided when the head is written**, and `Head` carries the `TrailerPolicy` (`conn` §2) for exactly this reason. `Forbid` is the default and permits a `Content-Length`. `PreferChunked` writes `Transfer-Encoding: chunked` and no `Content-Length`, so a later `Trailers` is expressible. The relay sets `PreferChunked` when the source may trailer and drops the source `content-length`.

`send_trailers` after a length-framed head is refused with `ConnError::StreamDone` — the engine rejects the call rather than emitting an illegal message. An inbound `Trailers` event is never an error; it simply cannot occur on a length-framed message.

**No body, whatever the policy says:**

| Message | Framing |
|---|---|
| `1xx` | no body, no `Transfer-Encoding` |
| `204` | no body, no `Transfer-Encoding` |
| `304` | no body; `Content-Length` may echo the selected representation's length |
| response to `HEAD` | no body; `Content-Length` may describe what would have been sent |
| `2xx` to `CONNECT` | no body; the stream becomes a tunnel |

A `TrailerPolicy` that conflicts with the table is ignored, not an error: the table is the protocol.

**Client serialization** is the mirror: `Target::Origin` with `Host`, `Target::Absolute` for a proxy request, `Target::Authority` only for `CONNECT`, `Target::Asterisk` only for `OPTIONS`. A request body is `Content-Length` (buffered) or chunked (streaming); `Expect: 100-continue` is sent when the caller asks.

---

## 5. `100-continue`

A request with `Expect: 100-continue` is parsed and its head emitted, but its body is not admitted until the application calls `Conn::body_wanted(stream)` (`conn` §5). The engine then writes `HTTP/1.1 100 Continue` and starts reading. A final response written first suppresses the `100` and the body is never read — that is the point of `Expect`.

`Expect` with any other value is `417`.

---

## 6. Keep-alive and reuse

HTTP/1.1 defaults to keep-alive; `Connection: close` on either side ends it. HTTP/1.0 requires `Connection: keep-alive` to reuse, and a 1.0 response without a length is close-delimited.

Reusable only when: every request reached its terminal byte, every response body was fully written with known framing, no `Connection: close` was seen or sent, and no error occurred. Any failure ends the connection after the current response; there is no resynchronisation on a malformed message.

Idle connections are closed by the `Idle` deadline, which `Conn` arms and refreshes (`conn` §6.6); the engine only decides when it applies.

---

## 7. Backpressure

h1 has no flow-control window, so no credit rides on a `Body` (`conn` §4: `sink` is `None`). Backpressure is the absence of reads: while a response body is unwritten, the engine stops consuming request bytes, and the client's send window closes. The cost is that one stalled exchange stalls the connection, which is why `proxy` never multiplexes streaming exchanges onto an h1 leg.

---

## 8. Tunnels

After a successful `CONNECT` or `101`, the engine emits `Event::Tunnel { stream }` and stops parsing HTTP. Bytes move through `read_tunnel`/`write_tunnel`; `end_tunnel` is the half-close, which on h1 is the connection's `shutdown_write` because the tunnel is the only stream. The engine does not inspect, frame, or limit tunnel bytes.

A client receiving `101` forwards `Connection` and `Upgrade` — they are not hop-by-hop in this one case (`proxy` §5).

---

## 9. Errors

| Condition | Server | Client |
|---|---|---|
| Malformed head, smuggling pattern, bad framing | `400`, close | `ProtocolError { connection: true, status: None }`, close |
| Head/field limits exceeded | `431`, close | same as above |
| Unsupported request `TE` final coding | `400`, close | n/a |
| Unsupported coding with chunked final | `501`, close | close-delimited response |
| Unsupported `Expect` | `417`, close | n/a |
| `Host` missing/duplicated on 1.1 | `400`, close | `ProtocolError`, close |
| Out-of-order `send_head` | `StreamDone` for the later stream | same |
| Body written after `End`/`Trailers` | `StreamDone`; the stream aborts, and the connection dies because bytes were already on the wire | same |

A malformed *response* has no status to send, which is why the client column is a connection-level `ProtocolError` rather than a status code. That asymmetry is the whole table's reason for two columns.

---

## 10. What a builder decides

Maximum head size, field count, chunk size and pipeline depth; whether the reason-phrase table is included; the `Method` token table beyond the constants.

Not theirs: the rejection list, CL/TE precedence, the duplicate-chunked rule, the no-body table, the trailer-at-head rule, synthetic ids and ordering, `100-continue` deferral, and the reuse conditions.
