# libhttp h1 — Design

**Status:** subsystem design. Owns HTTP/1.1 parsing, serialization, framing, pipeline ordering and tunnels.
**Parent:** `2026-10-26-libhttp-design.md`.
**Consumes:** `conn` (`Head`, `Fields`, `Event`, `StreamId`, `send_*`), transport (`Chunk`, `ChunkMut`).
**Consumed by:** `proxy`.

---

## 1. Scope

**Owns:** the request and status lines, field parsing, message framing (content-length, chunked, close-delimited), trailer sections, `100-continue`, keep-alive and reuse, pipeline ordering, `CONNECT` and `101` tunnels, and the parser's rejection rules.

**Does not own:** the event vocabulary (`conn`), body handles and credit (`conn`/transport), TLS, or worker dispatch. It has no socket and no clock: it is a byte machine.

**Two roles, one engine.** `h1::Server` and `h1::Client` differ in which start line they parse and which they emit; framing, fields, chunking and tunnels are shared code, not two implementations.

---

## 2. Streams and ordering

HTTP/1.1 has no stream ids. The engine assigns **synthetic ids in pipeline order**: 0, 1, 2, … increasing, one per request on a connection, never reused.

That single decision is what lets the proxy treat h1 like every other protocol:

- an `Event::Head` carries the id of the request it belongs to;
- responses must be emitted in ascending id order (HTTP/1.1 pipelining), enforced by the engine, which refuses to serialize a response for a later id while an earlier one is open;
- the connection carries one body per id at a time, so "per-stream" backpressure is not available — that is stated in `proxy`, not pretended here.

The engine tracks at most one *open* request at a time in the sense that matters for backpressure: a second pipelined request may be parsed, but its body cannot be admitted while an earlier stream's body is unconsumed.

---

## 3. Parsing

### 3.1 Lines

The request line is `method SP request-target SP HTTP-version CRLF`, with a single space between fields and none before the CR. The status line is `HTTP-version SP status-code SP reason-phrase CRLF`; the reason phrase is parsed for validity and discarded — it is not part of any event and is regenerated on output.

Bare `CR` or bare `LF` anywhere in the message head is fatal, as is a line longer than the configured maximum. `obs-fold` (a field continued on a line starting with SP/HTAB) is rejected, not unfolded: RFC 9112 permits rejection, and unfolding is a smuggling vector.

### 3.2 Fields

`name: OWS value OWS`. Whitespace **before** the colon is rejected — that is the historical `Transfer-Encoding : chunked` smuggling vector. Field names are tokens; field values exclude CTLs other than HTAB. Duplicates are preserved in wire order in `Fields` (`conn` §2); the engine decides which duplicates matter.

Limits: a maximum head size and a maximum field count, both configurable, both producing a `431` (or a `400` for a malformed line) and a close. There is no "truncate and continue".

### 3.3 Framing

Request framing, in order of precedence:

1. `Transfer-Encoding` present and ending in `chunked` → chunked. Any other final coding is `501`, and any `Transfer-Encoding` plus `Content-Length` is **rejected** (`400`, close).
2. `Content-Length` present → that many bytes. Multiple `Content-Length` values, or one value that is not a bare decimal, are rejected. `Content-Length: 5, 5` is rejected, not collapsed.
3. Neither → no body.

Response framing: `Transfer-Encoding` wins as above; `Content-Length` next; otherwise the body is **close-delimited** for a response, which means the connection cannot be reused.

Chunked framing: the size line is hexadecimal with an optional extension after `;` (extensions parsed and discarded); a chunk may not exceed the configured maximum; the final zero chunk is followed by a trailer section and `CRLF`.

### 3.4 What the parser rejects

Every one of these is a rejection, never a repair, and each is a fuzz target:

- `CL`/`TE` conflict, duplicated `CL`, a `CL` that disagrees with the body, a non-decimal or signed `CL`;
- whitespace before a field colon, `obs-fold`, bare CR/LF, NUL in a field value;
- a chunk size that is not hex, has a `+`, overflows, or exceeds the maximum;
- a missing final `CRLF` after the trailer section, or a trailer field that is itself forbidden;
- a request line with an unknown method *and* an invalid token, or with two spaces;
- a `Host` header missing on an HTTP/1.1 request, or more than one `Host`;
- an `HTTP/2` preface arriving on a connection the engine owns (that belongs to `conn`'s `Detect`, and reaching the h1 engine with it is a bug).

### 3.5 Undrained bodies

Whether a body was fully consumed is **not** a parser question — the parser never sees the application. The engine's keep-alive decision owns it: a connection may only be reused when every request on it reached its terminal byte and every response body was fully written. If the application ends a response with a body unread, the engine sets the response's `Connection: close`, and the connection is closed after it. There is no attempt to drain into the void, because draining an attacker-chosen length is the attack.

---

## 4. Serialization

The engine emits, in order: status line, fields, empty line, body. `Connection: close` is added when the engine has decided not to reuse; otherwise it is omitted (HTTP/1.1 default keep-alive). Response fields never include `Transfer-Encoding` unless the engine chose chunked.

Response body framing is decided **when the head is written**, because it cannot be changed afterwards:

- `Head` carries a `TrailerPolicy`. `Forbid` means a `Content-Length` may be written. `PreferChunked` means the engine writes `Transfer-Encoding: chunked` and does not write `Content-Length`, so a later `Trailers` is expressible.
- The relay sets `PreferChunked` whenever the source may produce trailers, and drops a source `content-length` when it does.
- A `Trailers` event after a `Content-Length` response is `ConnError::StreamDone` — the engine refuses rather than emitting an illegal message.

A body that ends without trailers uses `send_end`; with trailers, `send_trailers`. Both are terminal (`conn` §5).

---

## 5. `100-continue`

A request with `Expect: 100-continue` is parsed and its head is emitted, but its body is not read until the application asks for it. The application asks by calling `Conn::body_wanted(stream)` (`conn` §5); the engine then emits `HTTP/1.1 100 Continue` and starts admitting the body.

If the application sends a final response without ever calling `body_wanted`, no `100` is emitted and the request body is not read — which is the point of `Expect` and the reason this is not "always send 100 immediately".

An `Expect` value other than `100-continue` is `417`.

---

## 6. Keep-alive and reuse

HTTP/1.1 defaults to keep-alive; `Connection: close` on either side ends it. HTTP/1.0 requires `Connection: keep-alive` to reuse, and a 1.0 response without a length is close-delimited.

A connection is reusable only when all of these hold: every request on it reached its terminal byte, every response body was fully written with a known length or chunked framing, no `Connection: close` was seen or sent, and no error occurred. Any failure ends the connection after the current response; there is no resynchronisation attempt on a malformed message.

Idle keep-alive connections are closed by the `Idle` deadline (`conn` §6.6), which the engine arms and refreshes.

---

## 7. Tunnels

After a successful `CONNECT` or a `101`, the engine emits `Event::Tunnel { stream }` and stops parsing HTTP on that connection. Bytes move through `read_tunnel`/`write_tunnel` (`conn` §5), per stream, and the tunnel ends with `end_tunnel` in each direction.

- Half-close in one direction does not end the tunnel; the other direction keeps flowing.
- The engine does not inspect tunnel bytes, does not impose a framing, and does not apply header or body limits to them.
- On a TLS connection the half-close is `close_notify` plus `shutdown(SHUT_WR)`; the matrix is in `tls` §3.3, not restated here.

---

## 8. Errors

| Condition | Result |
|---|---|
| Malformed head, smuggling pattern, bad framing | `400` if a status can still be sent, then close |
| Head or field limits exceeded | `431`, then close |
| Unsupported `Transfer-Encoding` | `501`, then close |
| Unsupported `Expect` | `417`, then close |
| `Host` missing/duplicated on 1.1 | `400`, then close |
| Response body written after `End`/`Trailers` | `ConnError::StreamDone`, connection closed |

A parse error is fatal to the connection: HTTP/1.1 has no resynchronisation, and pretending otherwise is how desync attacks work.

---

## 9. What a builder decides

Maximum head size, field count, chunk size and pipeline depth; whether the reason phrase table is included (it is regenerated, so any valid phrase is legal); and the exact `Method` token table beyond the ones the library needs.

Not theirs: the rejection list, the CL/TE precedence, the trailer-at-head rule, synthetic stream ids and their ordering, `100-continue` deferral, and the reuse conditions.
