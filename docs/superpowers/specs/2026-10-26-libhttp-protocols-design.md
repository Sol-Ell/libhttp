# libhttp — Protocol Design (HTTP/1.1, HTTP/2, HTTP/3)

**Status:** part of the design for approval. **Date:** 2026-10-26.
**Parent:** [2026-10-26-libhttp-sync-http-design.md](2026-10-26-libhttp-sync-http-design.md).
**Companions:** [runtime](2026-10-26-libhttp-runtime-design.md), [execution](2026-10-26-libhttp-execution-design.md).
This document binds the wire behavior, parser strictness, framing, flow-control, compression and
security contracts of the three protocol engines. Engines are sans-I/O; they see byte-flow only.

---

## 1. Shared engine contract

- Engines are parameterized by role (server/client), take time as an argument, and emit output into a
  per-stream or per-connection sink. They never see sockets, threads, timers, or `Future`s.
- Parsing is incremental and bounded. Every length, count and accumulation is checked before it is
  used. Malformed input produces a typed `ProtocolError`; it never panics, indexes out of bounds, or
  allocates unboundedly.
- Framing headers (`Content-Length`, `Transfer-Encoding`, `Connection`) are library-owned. A handler
  setting `Transfer-Encoding` or `Connection` gets `Error::InvalidRequest`; there is no
  `Response::headers_mut` bypass, and at commit the engine re-filters those fields and collapses
  duplicate `Content-Length`. A handler-set single `Content-Length` is honored but a mismatch is a
  handler error. On HTTP/2/3, connection-specific fields are rejected at parse time.
- Header names and values are validated by the public constructors and by the parser
  (`HeaderName` token rules, `HeaderValue` rejecting CR/LF/NUL and other controls). Invalid values
  never panic: `Response::header` returns `Result<&mut Self, Error>`, and `HeaderValue::try_from`
  is the validating ingress. Static convenience values are validated at the same funnel.
- Typed headers use one trait, `Header: Send + Sync + 'static`, with `fn name() -> HeaderName`,
  `const MULTIPLE: bool`, `fn decode(HeaderRef<'_>) -> Result<Self, HeaderError>` and
  `fn encode(&self) -> Result<HeaderValue, HeaderError>`. The `fn name` allows boxed custom names.
  The raw head is parsed and framed on the reactor; typed decode runs when the exchange is handed to
  the handler/caller. A malformed known typed header is a `400`/stream error before the handler runs;
  `MULTIPLE` is enforced on input, so a duplicate singleton is `400`/stream error. Standard types have
  a generated known-name table; application types are registered with `register_header::<T>()`.
  Unregistered fields stay raw. Typed values live in a per-head arena bounded by
  `max_header_arena_bytes` and charged to the connection/global buffer caps. Application
  `decode`/`encode` panics are caught at the boundary and mapped to `400`/`500`.
- The same `Handler` may opt into deferred completion with
  `Request::defer(&mut self, &mut Response) -> Result<(DeferredRequest, DeferredResponse), Error>`. The move is
  atomic; the deferred handles support typed/raw headers, streaming request and response bodies,
  trailers, deadlines and cancellation. h1 keeps the connection parked; h2/h3 continue other streams.
  A failed defer (capacity/committed head) leaves the synchronous path intact.

---

## 2. HTTP/1.1

### 2.1 Parsing and strictness

Request line: method token (bounded), target (origin-form, absolute-form, authority-form, asterisk),
version (`HTTP/1.0`/`HTTP/1.1` only; `HTTP/0.9` and unknown versions rejected), CRLF required.
Header section: field-name tokens, `:` with no preceding whitespace, no obs-fold, no bare CR or LF, no
NUL/controls in names, bounded count (`max_header_count`), bounded total (`max_head_bytes`, 16 KiB;
a head spanning blocks is assembled into one bounded buffer).

Smuggling defenses (all are errors, not "best effort"):

- `Content-Length` together with `Transfer-Encoding` → reject.
- Multiple `Content-Length` fields, even with identical values → reject.
- Non-digit, signed, overflowing, or whitespace-padded length → reject.
- `Transfer-Encoding` whose final coding is not `chunked`, or with unknown codings → reject; more than
  one `chunked` → reject.
- Chunk size: hex only, bounded digits, overflow checked; chunk extensions parsed and discarded with a
  byte bound (`max_chunk_extension_bytes`, 1 KiB); chunk data followed by exactly CRLF.
- Trailers: bounded (`max_trailer_bytes`, 8 KiB), no `Content-Length`, `Transfer-Encoding`, `Host`,
  or other framing fields; parsed only after the last chunk.
- Absolute-form targets: the authority is authoritative; a conflicting `Host` is ignored for routing
  but retained for the handler (and never used to rewrite the target). Multiple `Host` fields are
  rejected (`400`) even when identical.
- Request-target and header values reject NUL and CTLs; obs-text (0x80–0xFF) is allowed in values
  only, never in names.
- A second request head arriving while a body is incomplete is not parsed until the body ends.
- CONNECT (`authority-form`) and `Upgrade`/`101` are rejected: `501` for CONNECT, `400` for an
  `Upgrade` that would require a tunnel. There is no h1 Upgrade to h2c; h2c is prior knowledge only.
- Typed decode happens per field line after the raw head passes framing/limit checks; a failed
  known-header decode produces `400` before the handler runs. Raw custom fields remain available as
  `HeaderRef` views; the typed arena is bounded by `max_header_arena_bytes`.
- A deferred h1 exchange keeps the connection and request-body stream alive but accepts no next
  request until the response completes; pipelining/ordering are unchanged. If the application drops
  the request body unread, the normal drain/close rule applies and the deferred response is
  cancelled.

### 2.2 Bodies, expect, trailers

- Request bodies: `Content-Length`, `chunked`, or none; `max_body_bytes` (8 MiB default) applies to
  the decoded body for both streamed and buffered handlers.
- `Expect: 100-continue`: the `100` is emitted when the handler first reads the body and the body is
  not already complete; any other `Expect` value gets `417`. A handler that responds without reading
  wins; the drain rule applies.
- Response bodies: the library chooses framing at head commit. No body for `HEAD`, `1xx`, `204`, `304`
  (writes are accepted and discarded; no framing is emitted). HTTP/1.1 uses `Content-Length` if the
  handler set one, else chunked; HTTP/1.0 uses `Content-Length` or closes the connection after the
  response. HTTP/1.0 keep-alive requires a `Content-Length`.
- Trailers: `finish_with_trailers` requires chunked framing; setting `Content-Length` and then trailers
  is a handler error. Request trailers become visible after body EOF.
- Unread request body on handler return: drain at most 64 KiB within 100 ms; then close the connection
  with `Connection: close` set on the response. Never block indefinitely to drain.
- A handler error before head commit becomes a minimal `500` with `Content-Length: 0`; after commit,
  the connection is closed (chunked framing cannot be terminated cleanly). A protocol error produces a
  minimal `400`/`408`/`413`/`431`/`501` when the head was parseable, then close.

### 2.3 Keep-alive, pipelining, connection lifetime

- HTTP/1.1 keep-alive is the default; `Connection: close` in the request or response closes after the
  response. HTTP/1.0 keep-alive requires explicit `Connection: keep-alive` and a known length.
- At most one exchange executes per connection. Up to `max_pipelined_requests` (8) following heads are
  parsed and queued behind it; beyond that the reactor stops reading. Responses are written strictly
  in request order. A malformed pipelined head after a valid one produces the error response after
  the earlier responses, then close.
- `Date` is generated per reactor per second; `Server` is off by default and configurable.
- h2c prior knowledge: the server peeks the first 24 bytes; an exact HTTP/2 client preface selects the
  h2 engine, otherwise HTTP/1.1. TLS selection is ALPN.

### 2.4 Client

The client parser enforces the same framing rules for responses: status line, header validation,
1xx interception (`100` ignored after body send; `103` ignored), chunked decoding, trailers,
`Content-Length` mismatch → error, no body for `HEAD`/`204`/`304`, and no `101`/CONNECT. Request
serialization uses `Content-Length` for buffered bodies and chunked for `send_with` streaming.

**Typed headers.** The client parses the raw response head, then decodes typed headers on the calling
thread before returning `ClientResponse`; `ClientBuilder::register_header::<T>()` registers application
types. Unknown fields stay raw. The same `Header`/`HeaderRef`/`HeaderValue` contracts as the server
apply.

**Early final response.** If the server sends a final response before the request body is finished
(for example `413`), the client stops the upload: h1 stops writing and may not be able to reuse the
connection, h2 resets its send side after the response, h3 stops/reset its send stream. The caller's
body writes fail with `Canceled`, and `send_with` discards the closure's error once a final response
has arrived, returning that response. A buffered `send` returns the early response directly. The
response is never lost because the local write failed.

---

## 3. HTTP/2

### 3.1 Frames and state

Full frame codec: DATA, HEADERS, PRIORITY (parsed and ignored), RST_STREAM, SETTINGS,
PUSH_PROMISE (a connection error; push is disabled), PING, GOAWAY, WINDOW_UPDATE, CONTINUATION.
Frame size validated against SETTINGS (`16384..=16777215`). Stream state machine enforces valid
transitions, including half-closed states; a violation is a connection error with the appropriate
code. SETTINGS and WINDOW_UPDATE values are validated (zero increment is an error; window overflow is
an error). GOAWAY's last-stream-id is honored; a graceful server shutdown sends GOAWAY and finishes
open streams.

### 3.2 HPACK

Full HPACK encoder/decoder: static table, dynamic table with eviction, incremental indexing, Huffman
coding, table-size updates, and the peer's `SETTINGS_HEADER_TABLE_SIZE`. Dynamic-table insertions are
bounded by the advertised table size (`Limits::h2_header_table_size`, default 4 KiB); decoded field
sections are bounded by `h2_max_header_list_bytes` (64 KiB **including** 32 bytes per field), and the
field count is bounded by `max_header_count` (100), so a compressed bomb cannot expand into unbounded
memory. Forbidden connection-specific fields
(`Connection`, `Keep-Alive`, `Proxy-Connection`, `Transfer-Encoding`, `Upgrade`) and invalid `TE`
values are rejected. Pseudo-headers must precede regular fields, appear once, and form a valid set;
`:path` is required except for CONNECT (which is rejected). Header names must be lowercase on the
wire. HPACK produces raw field lines; typed decode happens once at handoff through the same `Header`
trait, with the standard table and the registered extension registry, before the handler runs.

### 3.3 Flow control and per-stream isolation

Connection and stream windows are maintained per RFC 9113. With the defaults (32 streams,
`h2_initial_connection_window` 1 MiB), the connection window is partitioned into per-stream shares
(execution §6): a slow stream is throttled to its share, then reset on no progress; other streams
continue. Inbound DATA is charged to the stream queue and the connection quota; when a worker is
slow, the reactor withholds that stream's share of connection credit and its own stream credit; other
shares are unaffected. The per-exchange configured
buffers (64 KiB per direction) are ceilings; the effective **total** per stream is
`min(128 KiB, connection budget / open streams)`, split by demand, with borrowing. Connection credit for discarded DATA is returned when the
buffers are released. Outbound data is scheduled fairly across streams; a stream with no window does
not block other streams. Per-stream outbound buffers and HPACK encoder output are independent; a
single write sink is not assumed, and frame length prefixes are backfilled in place.

A deferred stream is not an executing stream: it keeps its flow-control windows and buffers but no
worker; other streams continue. The per-stream/connection caps and `max_deferred_exchanges` bound it,
and shutdown/deadline cancellation resets it. Completion is encoded/written by the application thread
and framed by the reactor, exactly like a handler-produced response.

### 3.4 Server and client behavior

- Client: `SETTINGS_ENABLE_PUSH = 0`, our `SETTINGS_MAX_CONCURRENT_STREAMS` is the configured value;
  the peer's limit is respected; one connection per authority (no certificate-based coalescing).
- Server: no push, no priority scheduling, extended CONNECT rejected, `MAX_CONCURRENT_STREAMS`
  advertised as configured; streams over the cap are refused, not queued.
- HTTP/2 and HTTP/3 requests carry `:authority`, not necessarily `Host`.
  `Request::header::<Host>()` synthesises a `Host` value from `:authority` when the field is absent;
  a raw `Host` entry exists only if the peer sent one. Duplicate `:authority` is a protocol error;
  a duplicate typed singleton field is a stream/connection error.
- Content-length: if present, the sum of DATA must match; mismatch is a stream error. Trailers are a
  final HEADERS with `END_STREAM`, no pseudo-headers.
- Unread request body: after the response, reset the stream (`RST_STREAM(CANCEL)`, h3
  `RESET_STREAM`/`STOP_SENDING`) immediately and keep the connection; no drain is attempted.
- Client response bodies: stream DATA is pulled by the worker; dropping a response before EOF resets
  the stream.
- 1xx: `100` interim responses are allowed before the final response; the client core consumes them
  (and sends its body after the first `100` only when it chose to wait, which v1 does not) and never
  exposes a 1xx as a `ClientResponse`. There is exactly one final response.

### 3.5 h2 security limits

`h2_max_header_list_bytes` (64 KiB), `h2_max_continuation_frames` (8), bounded SETTINGS/PING/RST
handling, stream-count limit, window-overflow checks, and header-table size limits. CONTINUATION
floods are cut off at the frame or byte limit with a connection error. Frame-level errors do not
panic.

---

## 4. HTTP/3

### 4.1 QUIC backend: chosen on merit, not obligation

`quinn-proto` 0.11.19 is the v1 backend because it is the only mature sans-I/O, pure-Rust QUIC
transport that uses rustls and has no tokio dependency; its driving API (owned `BytesMut` in,
caller-buffer `poll_transmit` out, `poll_timeout`) matches our reactor. That is a decision, not a
requirement from the brief.

Alternatives considered:

| Option | Verdict |
|---|---|
| `quiche` (Cloudflare) | Sans-I/O, but uses BoringSSL (a second TLS stack) and couples its own HTTP/3/QPACK; using its h3 would replace the h3/QPACK we are required to own. Phase-2 fallback only if quinn misses G3 or its copies dominate and the TLS-stack conflict is resolved. |
| `neqo-transport` (Mozilla) | Sans-I/O, but uses NSS (a second TLS stack and C dependency), not rustls. Rejected for v1. |
| `s2n-quic` | Tokio/async-oriented; rejected by the no-async constraint. |
| Hand-rolled QUIC | Explicitly rejected by the brief for v1. |

The h3 engine talks to a narrow internal QUIC surface (endpoint, connection, stream events, datagram
in/out, timers). quinn types stay inside one private module; the public API never sees them. If G3 or
the copy/syscall budget fails, the measured comparison against quiche decides whether to swap the
backend; the h3 framing/QPACK layer stays ours.

### 4.2 Wire behavior

- Request streams carry HEADERS (QPACK), optional DATA, optional trailing HEADERS; control streams are
  unidirectional (control, QPACK encoder, QPACK decoder); SETTINGS and GOAWAY are implemented; push,
  extended CONNECT, HTTP/3 datagrams, 0-RTT, connection migration and Alt-Svc are out.
- `SETTINGS_QPACK_MAX_TABLE_CAPACITY = 0` and `SETTINGS_QPACK_BLOCKED_STREAMS = 0`. Required Insert
  Count is always 0. The encoder uses static-table indices and literals only.
- The decoder enforces the cumulative decoded-size budget (`max_qpack_decoded_bytes`, 64 KiB) with
  32 bytes per field of overhead, the `max_header_count` field limit, rejects dynamic references as a
  connection error, and bounds control-stream instructions. A peer that inserts despite capacity 0 is in violation and is
  disconnected with the appropriate QPACK error.
- QPACK produces raw field lines; typed decode uses the same `Header` trait/registry at handoff,
  enforces `MULTIPLE`, and charges the arena to the connection/global caps.
- A deferred request stream keeps its QUIC stream/flow-control state without a worker; other streams
  continue, and the same deadline/cancellation rules apply. The application writes through the
  deferred response handle; the reactor frames/QPACK-encodes.
- Per-stream isolation does not mirror the h2 partition formula: quinn owns `max_data` and the
  connection/stream receive windows, so there is no library-advertised h3 connection window to
  divide. We pull stream bytes only when the worker queue has room, cap concurrent streams at
  `h3_max_concurrent_streams` (32), and reset a stream that is at its pull limit with no worker
  progress for `read_stall`; QUIC credit is returned by the reset. Worker backpressure therefore maps
  onto QUIC flow control without a h2-style share calculation.

### 4.3 Datagrams, timers, budget

Datagram batching is Linux `recvmmsg`/`sendmmsg` plus optional GSO/GRO; macOS/Windows send and
receive one datagram per operation in v1. Incoming datagrams are read into the `BytesMut` passed to
`Endpoint::handle` (one kernel copy; quinn splits into refcounted `Bytes` without a payload copy);
each datagram allocates one `BytesMut`, measured. Outgoing packets are encoded into the caller's
buffer (no extra `Vec`) and copied by the kernel. Timers come from
`Connection::poll_timeout()` into the reactor heap; there is no QUIC timer thread. G3 compares
against a pinned quiche h3 server on the same host.

### 4.4 Client

HTTPS requests use HTTP/3 only when `ClientBuilder::http3(true)` is set and the `http3` feature is
compiled; there is no Alt-Svc lookup, no Happy Eyeballs, no 0-RTT and no fallback. Plaintext never
uses h3. One QUIC connection per authority, multiplexed up to our advertised stream limit. Dropping a
response resets its stream; the connection stays.

---

## 5. Cross-protocol security tests

- HTTP/1.1 smuggling corpus (RFC 9112 §11 examples plus known vectors: CL/TE conflicts, duplicate CL,
  obs-fold, whitespace before colon, bare CR/LF, chunk-size oddities, trailer abuse, pipelining after
  errors).
- HTTP/2 conformance (h2spec) and frame/HPACK fuzzing; CONTINUATION, SETTINGS and PING floods;
  window-overflow; forbidden fields; pseudo-header ordering.
- HTTP/3 interop (curl/quiche) and frame/QPACK fuzzing; decoded-size budget; control-stream abuse.
- TLS: ALPN mismatch, truncation without `close_notify`, client-certificate failure, handshake
  timeout.
- Parser targets run per commit and in nightly long runs; no panic, no unbounded allocation, no hang
  on the corpus.
