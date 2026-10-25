# libhttp — Design

**Status:** draft for approval (revision 2). Nothing is implemented yet.
**Date:** 2026-10-25.
**Scope:** HTTP/1.1 and HTTP/2 in one non-blocking, synchronous Rust library, with a
concrete path to HTTP/3 and an early QUIC spike.

## 1. Intent

Rust's HTTP stack is framework-shaped: it assumes an async runtime, hides allocation
and copying behind trait objects, and makes the cost of a request hard to see or
predict. Migrating between runtimes is expensive because the runtime is woven through
the API, and `async`/`.await` makes control flow and cancellation harder to reason
about than the protocol requires. At the same time, "no async" is not an excuse for
thread-per-connection and blocking sockets: that trades the runtime's problems for
scheduler and memory problems at exactly the connection counts HTTP/2 is meant to
serve.

`libhttp` is a non-blocking, event-driven library, not a framework. One reactor owns
every socket. Protocol engines turn readiness and bytes into typed messages and queued
writes. Application handlers run on a worker pool and never touch a socket. There is no
async runtime, no `Future`, and no blocking socket I/O anywhere in the library.

The one-sentence rule: **every abstraction on the hot path must pay for itself in
measured cost, and every copy, allocation, wakeup, or syscall must be visible in the
API or the documentation.**

### Abstraction stance

Traits, generics, and the occasional trait object are fine. They are a problem when
they force a structure on the user (a `Service` tower, a mandatory `Body` trait, a
runtime) or when they sit on the per-field or per-byte path. This design uses:

- a `Transport` trait for byte-stream transports (TCP now, TLS later),
- a `Session` trait for things the reactor drives (one virtual call per event, not per
  byte),
- a `Handler` trait for application code (one call per exchange),
- concrete, generic engines underneath where dispatch cost would be measurable.

It does not use a generic frame/body/service abstraction to unify protocols. HTTP/1.1,
HTTP/2, and HTTP/3 are concrete engines with different semantics; shared code is data
types, buffers, and the reactor.

Dependencies are not a sin either. `libringbuf` supplies the contiguous ring buffers
this design wants; `bytes` supplies refcounted chunks where a ring cannot hand out
stable memory; `libc` supplies `epoll` and socket options; `rustls` and `quinn-proto`
are candidates for TLS and QUIC. The HTTP protocol engines and all framing, parsing,
serialization, HPACK, and QPACK are ours. Delegating QUIC transport is not delegating
the HTTP library.

### What "typed, not strings" means here

- `Method`, `StatusCode`, `Version`, and header names are typed; unknown or custom
  values are byte slices with explicit escape hatches, never `String`.
- A request target is parsed into its forms (`origin`, `absolute`, `authority`,
  `asterisk`) with path/query as borrowed bytes; no percent-decoding at this layer.
- Header lookup is a linear scan over interned names with a small inline index, not a
  hash map per message.
- Bodies are streaming handles with explicit framing, not `Vec<u8>` collected from the
  socket.
- Parsed messages borrow connection-owned storage. Crossing a thread boundary is an
  explicit move of an arena or a refcounted chunk.

## 2. Goals

1. HTTP/1.1 and HTTP/2 server and client over non-blocking sockets, plus TLS later.
2. One reactor architecture: no thread per connection, no blocking `read`/`write` on a
   socket anywhere in the library.
3. Public low-level types (reactor, transport, connection, stream/events) so a reverse
   proxy, a protocol tester, or an embedded use can be built without forking.
4. A worker model that lets ordinary synchronous handlers exist without stalling the
   reactor.
5. Typed, protocol-neutral messages that HTTP/3 can reuse, with QPACK-shaped storage
   in mind.
6. Correctness against the RFCs, request-smuggling defenses, fuzzed parsers.
7. Measured performance: 10k idle connections per reactor, bounded memory, published
   syscall/allocation/copy budgets and a comparison against hyper.

## 3. Non-goals

- Async runtimes, `Future`, `Waker`, `Pin`, task spawning.
- Blocking socket I/O and thread-per-connection, including a "simple blocking mode".
- A routing framework, middleware stack, extractors, or dependency injection.
- Caching (RFC 9111), content negotiation, compression, cookies, sessions.
- WebSocket framing; upgrades and `CONNECT` detach to an opaque byte tunnel.
- HTTP/2 push and priority; extended `CONNECT` (RFC 8441) is out of v1.
- `no_std`; the library is `std`-based.
- Windows and macOS in v1. Linux first; portability comes after the design is proven.
- A hand-written QUIC stack in v1. QUIC transport is delegated to a sans-io Rust
  implementation; HTTP/3 framing and QPACK are ours. This is revisited only if the
  dependency proves unacceptable.

## 4. Anti-goals

- An ergonomic API that hides a copy, an allocation, a wakeup, or a syscall.
- A blocking call on the I/O path, however convenient.
- A thread per connection, however simple.
- Framework-shaped abstractions forced on the user.
- A trait object on the per-header or per-byte path.
- Abstraction that only pays off for HTTP/3.
- Benchmark claims without the harness, hardware, and versions beside them.

## 5. Constraints and decisions from the conversation

- HTTP/1.1 and HTTP/2 are required; HTTP/3 is future work and its direction is decided
  now.
- Platform-specific features are allowed; hiding the OS is not a goal.
- Traits, allocation, and threads are minimized, not banned. Their use must be
  justified and visible.
- `libcommon` is not a dependency. `libringbuf` is, and other dependencies are allowed
  when they earn their place.
- Low-level transport types are public. The proxy is the acid test for their
  completeness.
- Handlers run on a worker pool. Stackful coroutines are rejected.
- Thread-per-connection and blocking I/O are rejected.
- `async`/`await` is rejected outright.
- TLS lands after the engines and proxy.

## 6. Approaches considered

### 6.1 Message API shape

**A — transport mirror.** `HttpListener` yields an `HttpStream` that owns a socket;
`read_message`/`write_message`; `HttpServer`/`HttpClient` above. Clean for HTTP/1.1,
false for HTTP/2 (one connection, many streams, shared HPACK and flow control), and it
forces two high-level models. Rejected.

**B — connection + exchange handles (chosen).** A `Connection` is one protocol engine
for one transport connection; a `Stream` is an exchange handle into it. HTTP/1.1 has
one implicit stream at a time, HTTP/2 many, HTTP/3 many over QUIC streams. The
high-level server/client share policy, not protocol structure.

### 6.2 Execution model

**All async.** Rejected: runtime coupling, `Future` machinery, and the exact thing this
library exists to avoid.

**Thread per connection, blocking I/O.** Simple, fast at low concurrency, and wrong
here: one thread per idle connection, a blocked handler stalls every stream on an
HTTP/2 connection, blocking writes deadlock against clients that are still sending, and
10k connections cost 10k stacks and context switches. Rejected outright.

**Chosen: one reactor, non-blocking I/O, worker pool for application code.** The
reactor owns every socket, uses `epoll` edge-triggered readiness, and never runs a
handler. Handlers run on a bounded worker pool and exchange data with the reactor
through bounded queues. The reactor may block in `epoll_wait` when idle — that is
waiting, not I/O — and workers may block on their own databases or on a queue, but no
thread ever blocks on a socket.

**Stackful coroutines.** A way to keep handlers non-blocking without channels. Rejected:
stacks, context switching, unsafe scaffolding, and a scheduler the library does not
otherwise need.

### 6.3 I/O substrate

**mio.** `mio::net` sockets are non-blocking by construction; mio has no blocking mode
and `Poll` is a readiness API. Its useful ideas are the token→slab mapping, edge-
triggered interest, and drain-until-`WouldBlock`. For a Linux-first library, direct
`epoll` via `libc` avoids a wrapper and gives control over batching, timers, and
`eventfd`. The reactor interface leaves room to add a mio backend later for portability
(macOS/Windows), but v1 does not depend on mio.

**io_uring.** A completion-based driver is a later option behind the same `Session`
interface; it is not v1. The engine API does not assume readiness beyond its
`on_readable`/`on_writable` adapter.

## 7. Architecture

```
              +------------------------------------------------------+
  app ------> | Server / Client (policy, pooling, dispatch)          |
              +------------------------------------------------------+
                                  |
              +------------------------------------------------------+
              | h1 engine | h2 engine | h3 engine (later)            |  non-blocking
              | events, framing, HPACK/QPACK, flow control           |  state machines
              +------------------------------------------------------+
                    |                                   |
              +---------------------------+     +-------------------+
              | Reactor (epoll, slab,     | <-> | Worker pool       |
              | timers, eventfd, queues)  |     | (handlers only)   |
              +---------------------------+     +-------------------+
                                  |
              +------------------------------------------------------+
              | Transport: Tcp | Tls (later) | Quic (quinn-proto)    |
              +------------------------------------------------------+
```

The reactor is the only owner of file descriptors. Engines never spawn threads and
never block on a socket. Workers never see a file descriptor: they receive owned heads
and body chunks, and return response commands. That is the whole concurrency model.

### 7.1 Reactor

```rust
pub struct Reactor { /* epoll fd, slab, timer heap, eventfd, command queue */ }

pub trait Session {
    fn fd(&self) -> RawFd;
    fn interest(&self) -> Interest;
    fn on_readable(&mut self) -> Result<()>;
    fn on_writable(&mut self) -> Result<()>;
    fn on_timeout(&mut self, now: Instant) -> Result<()>;
    fn next_deadline(&self) -> Option<Instant>;
}

impl Reactor {
    pub fn new(config: ReactorConfig) -> io::Result<Self>;
    pub fn register(&mut self, session: Box<dyn Session>) -> ConnId;
    pub fn run(&mut self) -> io::Result<()>;
    pub fn handle(&self) -> ReactorHandle; // wake(), post(command)
}
```

- `epoll` edge-triggered; every readable event drains until `WouldBlock`; every writable
  event flushes until `WouldBlock` or the write queue is empty. `EPOLLRDHUP`/`EPOLLHUP`
  are handled explicitly.
- `Token` indexes a slab slot with a generation to prevent stale events after close.
- A timer heap stores per-connection and per-stream deadlines; `epoll_wait` uses the
  nearest deadline as its timeout, so there is no timer thread.
- `eventfd` wakes the loop for worker commands: response headers/chunks, flow-control
  releases, cancellations, and new streams. Commands land in a bounded MPSC queue and
  are drained in batches. One wake per batch, not per byte.
- A per-iteration budget (events, frames, bytes, handoffs) keeps one busy connection
  from starving the rest.
- Accept is a `Session` too: the listening socket is registered readable; the accept
  loop is part of the reactor, not a separate thread.
- One reactor per core by default (`SO_REUSEPORT` via `reactors = n`); each has its own
  slab and timer heap. Cross-reactor work is not needed for v1.

`Session` is the one dynamic abstraction the reactor needs: `Box<dyn Session>` costs a
vtable call per event, nothing per byte. It lets the reactor drive h1, h2, h3, the
listener, a tunnel, or a proxy session uniformly.

### 7.2 Transport

```rust
pub trait Transport {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize>;       // non-blocking
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize>;
    fn flush(&mut self) -> io::Result<()>;
    fn shutdown(&mut self, how: Shutdown) -> io::Result<()>;
    fn alpn(&self) -> Option<&[u8]>;
    fn fd(&self) -> RawFd;
}
```

All implementations are non-blocking and return `WouldBlock` rather than waiting.
`TcpTransport` wraps a `std::net::TcpStream` created non-blocking (`SOCK_NONBLOCK`),
with `TCP_NODELAY`, `SO_REUSEPORT`, and keepalive configured. `TlsTransport` (rustls)
arrives later and decrypts/encrypts on top of TCP, buffering records internally; it
also returns `WouldBlock` when a record is incomplete.

QUIC is not a `Transport`: it is datagram-based and multiplexes connections over one
UDP socket. The HTTP/3 section defines a separate endpoint/connection integration.

### 7.3 Buffers

`libringbuf` is the buffer primitive:

- **Write buffers (all protocols):** `RingBuf<u8>`. The spare region is one slice, so
  response heads and body bytes serialize directly into it; the readable region is one
  slice, so `send`/`writev` sees contiguous bytes and consumption is `drain(..n)`.
  No compaction, no wrap-around split, no `Vec` shifting per write.
- **HTTP/1.1 read buffer:** `RingBuf<u8>`. The spare region is passed to `recv`, the
  readable region is parsed in place, and consumed bytes are drained. The parser never
  copies the buffer to make room.
- **HTTP/2 read buffer:** `bytes::BytesMut` and pooled `Bytes`. DATA payloads must
  outlive the `on_readable` call and cross a thread boundary to a worker, so they must
  be refcounted. A ring cannot hand out stable slices while it is being refilled; a
  `BytesMut` split into `Bytes` does that without copying. The write side still uses a
  ring.
- **HTTP/3:** `quinn-proto` owns stream buffers and hands out refcounted chunks; the h3
  engine reads them directly.

Sizes: active read buffer 32 KiB, idle read buffer 4 KiB (shrunk when a connection goes
idle, grown on demand), write buffer 32 KiB, per-stream HTTP/2 header arena bounded by
the header-list limit. Buffers are pools or per-connection allocations, never
per-request allocations after warm-up.

Copies are counted:

- HTTP/1.1 low-level/proxy path: kernel→ring, ring→kernel; no intermediate buffer.
- HTTP/1.1 worker path: kernel→ring, ring→`Bytes` chunk (one copy to cross threads),
  worker→response queue→ring, ring→kernel.
- HTTP/2: kernel→`BytesMut`, split→`Bytes` (no copy) to worker; response→ring→kernel.
- Header bytes: HTTP/1.1 one bounded copy into the arena; HTTP/2/3 decoded once into
  the arena.

### 7.4 Message model

- `HeaderName<'a>`: `Known(HeaderId)` for the standard set plus common extension names,
  `Custom(&'a [u8])` otherwise; comparison by ID, then case-insensitive bytes.
- `Headers`: ordered field lines preserved exactly, duplicates included, with a
  case-insensitive lookup index (default 32 inline slots). Nothing is merged into a
  map; `Set-Cookie` is never combined.
- `Method`, `StatusCode`, `Version`, `RequestTarget<'a>` as before.
- `BodyLength { None, Fixed(u64), Chunked, UntilClose, Tunnel }` for HTTP/1.1;
  trailers are a separate end-of-body field set, never merged.
- `RequestHead` is protocol-neutral. HTTP/2 and HTTP/3 pseudo-headers become an
  `Absolute` target (`:scheme`, `:authority`, `:path`); no synthetic `Host` field is
  created, and `authority()`/`scheme()`/`path()` consult the target first and `Host`
  second. One handler works for all protocols.
- HTTP/2 and HTTP/3 encoding lowercases field names through a byte table without
  allocating and rejects connection-specific fields; the serializer filters them or
  returns an error rather than emitting a malformed message.
- Header storage is an arena copied once from the read buffer (HTTP/1.1) or filled by
  HPACK/QPACK decoding (HTTP/2/3). It moves with the exchange; there are no
  self-referential structures.
- A worker gets the arena plus a body stream; the reactor keeps the socket and the read
  buffer. Nothing in an `OwnedHead` points into a buffer the reactor will refill.

### 7.5 HTTP/1.1 engine

A non-blocking state machine: `ReadHead → Body → Write → KeepAlive | Close | Tunnel`.
The engine owns its transport and ring buffers and is driven by the reactor.

```rust
pub struct Connection<T: Transport> { /* transport, read ring, write ring, parser, state */ }
impl<T: Transport> Connection<T> {
    pub fn new(transport: T, config: Config) -> Self;
    pub fn on_readable(&mut self) -> Result<()>;   // recv until WouldBlock, parse
    pub fn on_writable(&mut self) -> Result<()>;   // flush ring until WouldBlock
    pub fn on_timeout(&mut self, now: Instant) -> Result<()>;
    pub fn next_event(&mut self) -> Option<Event>; // Request, Data, End, Closed, Upgrade
    pub fn body(&self, data: DataRef) -> &[u8];    // borrowed view into the ring
    pub fn consume(&mut self, data: DataRef);
    pub fn respond(&mut self, status, headers, body) -> Result<()>;
    pub fn write_body(&mut self, buf: &[u8]) -> Result<usize>; // partial by design
    pub fn finish(&mut self) -> Result<()>;
    pub fn detach(self) -> (T, ReadBuffer);        // CONNECT / 101
    pub fn interest(&self) -> Interest;
    pub fn deadline(&self) -> Option<Instant>;
}
```

Protocol behavior is unchanged from the earlier design:

- Framing resolved at end-of-headers from method/status/version/fields; HEAD, 1xx, 204,
  304, and 2xx CONNECT have no body; `Chunked` must be final; conflicting CL/TE is fatal;
  duplicate CL only as an equal comma-list; chunk sizes overflow-checked; obs-fold,
  whitespace before a colon, and bare CR rejected; bare LF rejected deliberately.
- Headers: `Host` required exactly once on HTTP/1.1 requests; absolute-form wins; same-
  name order preserved.
- Pipelining: bytes beyond the current request stay in the ring; one request is parsed
  at a time and responses are written in order; the next head is not parsed until the
  previous exchange is complete.
- `Expect: 100-continue`: the engine never waits. The first body read or an explicit
  `continue_()` queues `100`; a final response before that skips it.
- CONNECT/`101`: `detach()` returns the transport and buffered tunnel bytes, leaving the
  engine inert; the reactor replaces the session with a tunnel session. A reactor
  adapter keeps the detached transport in an `Option` and hands it to the replacement,
  so the `Session` trait itself does not need a move-out method.
- HTTP/1.0: close-delimited responses, no chunked, no 1xx, keep-alive only by token.
- Low-level backpressure: `write_body` may accept fewer bytes than offered when the ring
  is full; the caller gets a `Writable` event and finishes later. The engine never
  blocks.

### 7.6 HTTP/2 engine

One connection, many streams, per RFC 9113. The engine is driven by the reactor and
produces events; it never runs a handler and never blocks.

- Preface and SETTINGS handshake; h2c is prior-knowledge only (RFC 9113 removed
  `Upgrade`); no `101`.
- Frames: DATA, HEADERS, PRIORITY (ignored), RST_STREAM, SETTINGS, PUSH_PROMISE
  (rejected), PING, GOAWAY, WINDOW_UPDATE, CONTINUATION; unknown types ignored; padding
  accepted, counted, discarded.
- Stream state machine, monotonic IDs, concurrency limits, RST/GOAWAY semantics.
  HEADERS blocks may span CONTINUATION frames and nothing may interleave.
- Message semantics and validation as in revision 1: pseudo-headers, lowercase names,
  trailers as a final HEADERS, `content-length` matching DATA, forbidden
  connection-specific fields, `TE: trailers` only, Cookie joined with `"; "` when
  translating to HTTP/1.1.
- Flow control: DATA only, padding counted. The stream window is replenished when the
  consumer takes the bytes (chunk dropped or `consume(n)`); the connection window is
  replenished as DATA is buffered into a bounded per-stream queue, up to a global
  buffered-bytes cap, so one slow stream cannot stall the connection. Zero/overflow
  increments are errors; a settings change moves every stream window by the delta and
  may make it negative.
- SETTINGS: own sent first, peer applied in order, unknown ignored, ACK immediately;
  server never sends ENABLE_PUSH; client sends 0; HEADER_TABLE_SIZE takes effect after
  ACK and forces a dynamic-table size update.
- HPACK: full static/dynamic tables and Huffman; 32-byte entry overhead; size update
  only at block start; EOS forbidden and padding rules enforced; discarded blocks are
  still decoded to keep the table in sync; encoder uses static + Huffman + a small
  bounded dynamic table with pooled entries.
- Writes: control frames first, then DATA round-robin across positive-window streams;
  field blocks are atomic queue items; frames coalesce into one `writev`.
- Errors: one GOAWAY with the highest peer stream acted on on connection error; RST on
  stream error; no RST in response to RST; if one frame causes both, connection error
  wins.
- Graceful shutdown: `GOAWAY(2^31-1, NO_ERROR)`, drain, then the final GOAWAY with the
  real last stream.
- Early response: allowed; remaining DATA is discarded and the stream reset NO_ERROR
  after the response when it cannot be drained.

### 7.7 Worker model

Handlers are synchronous functions on a bounded pool. They never see a file descriptor
and never perform socket I/O; they read body chunks from a bounded queue and write
responses into another. Blocking on those queues (or on a database) is fine — that is
what workers are for — and is distinct from blocking socket I/O.

```rust
pub trait Handler: Send + Sync + 'static {
    fn handle(&self, request: &mut Request<'_>, response: &mut Response<'_>);
}
```

`Request<'_>` is a borrowed view over the owned head arena plus a body stream enum
(`H1 | H2 | H3`). `Response<'_>` is a writer enum over the protocol resp. Body reads and
response writes may block on the queues; when the queue is full, the reactor stops
reading the socket (TCP) or withholds flow-control credit (HTTP/2/3), which is the real
backpressure. A bounded queue is the only place a worker can wait.

- HTTP/1.1: one job per exchange. The reactor parses the head, copies each body chunk
  into a `Bytes` and pushes it to the job; when the job's queue is full, the reactor
  stops reading that connection. The worker returns headers/chunks/trailers through a
  bounded command queue; the reactor serializes them and flushes. A slow peer fills the
  write ring, stops command consumption, and eventually hits the stream deadline.
- HTTP/2: one job per stream. The reactor delivers `DataChunk`s (refcounted, credit on
  drop) and receives `Responder` commands. Per-stream queues are bounded by the receive
  window and the global cap.
- HTTP/3: one job per request stream; same shape over QUIC stream chunks.
- The pool is shared and bounded. When the dispatch queue is full, HTTP/2/3 streams are
  refused with `REFUSED_STREAM`; HTTP/1.1 keeps the head buffered and stops dispatching
  until a worker frees, under a request deadline. Optional `503` shedding.
- Cancellation: a reset/closed stream makes every worker method return `StreamClosed`;
  the handler may finish into a discarded output. Dropping a `Responder` resets the
  stream. A response that cannot flush before `write_timeout` is reset (`CANCEL`), so a
  slow peer cannot pin workers forever.
- A panicking handler is caught: HTTP/1.1 closes the connection, HTTP/2/3 resets the
  stream. Handler execution is not timed out by the library; a stuck handler pins its
  worker, which is why the pool is bounded.

### 7.8 Server

Reactor(s) + worker pool. No accept thread, no connection thread.

- Listener session accepts, sets `TCP_NODELAY`/keepalive/`SO_REUSEPORT`, and negotiates:
  `H1`, `H2PriorKnown`, or `Both` (peek 24 bytes with `MSG_PEEK`, nothing consumed).
- HTTP/1.1: idle keep-alive connections live in the slab with a shrunken read buffer.
  On a complete head, the reactor builds the owned head and dispatches one job; body
  chunks follow; the response commands come back through the command queue. The
  connection stays in the reactor throughout.
- HTTP/2: stays in the reactor; per-stream jobs.
- Limits: connections, per-IP, concurrent streams, body size, header size, request
  deadlines, idle and write deadlines. `body (server)` is an optional total-body cap
  (`None` = unlimited) enforced by the server, not by the framing layer.
- Graceful shutdown: stop accepting, stop dispatching, close idle HTTP/1.1, GOAWAY and
  drain HTTP/2, deadline, reset. A cloneable `Shutdown` handle triggers it.
- Load shedding and queue policies are configuration, not hidden behavior. The
  high-level server does not expose CONNECT/`101` tunnels in v1; the low-level API
  does, and the proxy uses it.

### 7.9 Client

`Client` owns a small reactor (one thread by default) and a pool.

- `send` submits a request, then waits on a bounded response channel. The caller blocks
  — it is a synchronous API — but the socket is driven by the reactor and never
  blocked on by the caller.
- HTTP/1.1: a pool of idle connections keyed by origin; one in-flight request per
  connection; the per-origin lock is held only to take/return an idle connection, never
  across I/O.
- HTTP/2: connections are shared and multiplexed; per-stream queues are bounded and the
  reactor withholds credit instead of blocking when a caller is slow; dropping a
  response sends `RST_STREAM(CANCEL)`.
- No redirects, retries, cookies, or compression. One opt-in stale-connection retry.
- Connection failures, timeouts, and resets surface as typed errors.

### 7.10 Proxy

`examples/proxy` is a working reverse proxy built only on public low-level types, with
no worker pool and no blocking I/O:

- One reactor drives client and upstream connections; bidirectional pumping is a state
  machine over `on_readable`/`on_writable` and the engines' events. Each side is a
  separate `Session` because `Session` owns one fd; the pair shares an
  `Rc<RefCell<ExchangeState>>`, which is safe and lock-free in practice because a
  reactor is single-threaded. No multi-fd session is needed.
- HTTP/1.1 ↔ HTTP/1.1, HTTP/1.1 ↔ HTTP/2, HTTP/2 ↔ HTTP/2. HTTP/2 streams map 1:1;
  DATA is forwarded chunk by chunk and client credit is released only after the
  upstream write succeeds (connection credit per the bounded-buffer rule). Trailers,
  RST_STREAM, and GOAWAY propagate.
- CONNECT/`101`: detach both sides into tunnel sessions and pump opaque bytes.
- Strip `Connection`-named fields, regenerate `Host` from an absolute target, forward
  1xx responses it did not generate, never reorder fields, never reuse an upstream
  connection to an HTTP/1.0 client.
- Its integration tests are the completeness test for the low-level API. If the proxy
  needs a private API, the design failed.

### 7.11 HTTP/3 direction

HTTP/3 is not HTTP/2 over a different socket. It runs over QUIC: one UDP endpoint, many
connections, many streams, no head-of-line blocking at the transport, TLS 1.3 built in.
The design must not assume a byte-stream socket, which is why the QUIC integration is
separate from `Transport`.

**QUIC transport: delegated, deliberately.** `quinn-proto` is a deterministic,
sans-io QUIC implementation: no networking code, no timestamps from the OS, designed
for a foreign event loop. That is exactly the reactor integration wanted. It brings
loss recovery, congestion control, packet protection, and rustls handshakes. We
implement HTTP/3 framing, QPACK, control streams, and the request/response mapping.
`quiche` is the alternative: mature and bundled with an h3 module, but it would delegate
the HTTP/3 engine too and pull C/BoringSSL into the build. A hand-written QUIC stack is
a separate project, not a milestone of this one.

**Shape.**

- `QuicEndpoint`: owns the UDP socket and `quinn_proto::Endpoint`, demultiplexes
  datagrams, routes `ConnectionEvent`s, polls transmissions, and schedules the earliest
  QUIC timer. One UDP fd per endpoint, registered with the reactor.
- `h3::Connection`: wraps one QUIC connection. Client requests open a client-initiated
  bidirectional stream; server requests arrive on one. A control stream carries
  SETTINGS and GOAWAY; unidirectional QPACK encoder/decoder streams carry table updates.
  Frames: DATA, HEADERS, SETTINGS, GOAWAY, MAX_PUSH_ID (push is not implemented),
  CANCEL_PUSH, unknown frames ignored.
- Reuses the typed head, `Headers`, trailers, and body model. QPACK (static table,
  dynamic table, Huffman, blocked-stream limits) is a sibling of HPACK, not a rewrite
  of the message layer.
- Request streams are the h3 `Stream`; flow control comes from QUIC stream and
  connection windows. The worker model is identical: head arena to the job, body chunks
  in, response commands out, credit released when chunks are dropped.
- Listener equivalent: `QuicEndpoint` on UDP; ALPN `h3`; no TCP accept. `Server`/`Client`
  configuration gains `h3` later, behind a feature flag.

**Start now (spike H0, timeboxed, no production commitment).**

1. Pin `quinn-proto` and drive it from our reactor on loopback: endpoint → handshake →
   one bidirectional stream → echo bytes, with timers and `poll_transmit`.
2. Prototype the h3 frame codec (DATA/HEADERS/SETTINGS/GOAWAY) and a QPACK static-table
   decoder, fuzzed against known vectors.
3. Verify the abstractions: no `fd` on the h3 connection, no assumption of one
   connection per socket, no TCP-style write backpressure.

The spike exists to de-risk the direction and to keep the h1/h2 layers honest about
what they share. Production h3 comes after TLS.

## 8. Public surface

```
libhttp
  reactor     { Reactor, ReactorHandle, Session, Interest, ConnId }
  transport   { Transport, TcpTransport, TlsTransport, HttpListener, Accepted, Protocol }
  buffer      { ReadBuffer, WriteBuffer }        // libringbuf wrappers
  message     { Method, StatusCode, Version, HeaderName, Header, Headers,
                RequestHead, RequestTarget, Body, BodyLength }
  h1          { Connection, Event, DataRef, ResponseWriter, Config }
  h2          { Connection, Event, Events, StreamId, DataChunk, Responder,
                ResponseWriter, DataStream, ErrorCode, Config }
  h3          { QuicEndpoint, Connection, StreamId, ErrorCode, Config }   // later
  server      { Server, Handler, Request, Response, ServerConfig, Shutdown }
  client      { Client, ClientConfig, Request, Response }
  worker      { Pool, PoolConfig, BoundedQueue }
  error       { Error, ProtocolError, ConnectionError, StreamError, Limit, Timeout }
```

Feature flags: `server`, `client`, `reactor` (default), `tls` (later), `h3` (later).

## 9. Performance model and measurement

Targets, all measured and published:

- 10k idle connections of either protocol per reactor thread, with measured RSS.
  Idle costs a slab slot, a small ring, and a timer entry — not a thread.
- HTTP/1.1 low-level request: two data syscalls (one `recv`/`recvmsg`, one
  `sendmsg`/`writev`) plus the amortized `epoll_wait`; worker mode adds a bounded queue
  wake, batched where possible.
- HTTP/1.1 worker path: zero heap allocations per request after warm-up (head arena
  from the pool, `Bytes` chunks from a pool, response commands from a queue).
- HTTP/2: bounded per-stream memory; stream credit released on consumption; connection
  credit against a bounded aggregate buffer; no unbounded write queue.
- Copies: one kernel→user and one user→kernel per direction; the only extra copy is
  HTTP/1.1 body bytes crossing to a worker (documented and measured). HTTP/2 DATA
  crosses without copying.
- Throughput/p50/p99 in the same range as hyper for 1 KiB responses at equal
  concurrency, and better tail behaviour under slow consumers; the harness, versions,
  and hardware ship with the numbers.

Harness: `criterion` for parsers, ring buffers, and HPACK/QPACK; `oha`/`wrk2` for
end-to-end; `strace -c` for syscall counts; a counting allocator for allocations;
`perf` for profiles; a separate `tools/bench-compare` crate (path dependency on
libhttp, hyper as its only heavy dependency) runs both behind the same load generator.

## 10. Correctness, safety, limits

- Malformed input is an error, never a panic. Errors map to the right HTTP/1.1 status
  (`400`, `408`, `413`, `431`, `505`) when a response is possible, otherwise close.
- Fuzzed: HTTP/1.1 request/response/chunked, HTTP/2 frames, HPACK, QPACK, h3 frames, and
  the public message types.
- A smuggling corpus: CL/TE conflicts, duplicate CL, whitespace variants, obs-fold,
  bare CR/LF, whitespace-split start lines, chunk-size oddities, undrained bodies.
- Conformance: unit/table-driven tests; `h2spec`/`h2load`; curl/nginx interop; QUIC/h3
  interop later.
- Limits checked eagerly: header block, header count, request line, body, chunk size,
  trailer block, frame size, concurrent streams, HPACK table, header list, read-ahead,
  global HTTP/2 buffered DATA. Defaults:

  | Limit | Default |
  |---|---|
  | request line | 8 KiB |
  | header block | 16 KiB |
  | header count | 100 |
  | body (server, optional) | 2 MiB |
  | chunk size | 16 KiB |
  | read buffer | 32 KiB active / 4 KiB idle |
  | write buffer | 32 KiB |
  | HTTP/2 frame | 16 KiB |
  | HTTP/2 concurrent streams | 100 |
  | HTTP/2 header list | 16 KiB |
  | HTTP/2 HPACK table | 4 KiB |
  | HTTP/2 stream window | 64 KiB |
  | HTTP/2 connection window | 1 MiB |
  | HTTP/2 global buffered DATA | 8 MiB |
  | timeouts (header/body/idle/write) | 10s / 30s / 60s / 30s |

- Unsafe is confined to the reactor syscalls, the ring buffer read path, and the QUIC
  adapter, with safety comments. `unsafe_op_in_unsafe_fn` denied, `missing_docs` warned.
- No global mutable state; threads exist only as configured reactors, workers, and the
  client driver. No logging by default; counters exposed as a snapshot.

## 11. Crate layout and dependencies

Single crate, modules as in §8. One crate rather than a workspace until compile time or
feature isolation demands otherwise.

- `libringbuf` (path dependency during development; publish or vendor before release):
  ring buffers.
- `bytes`: refcounted HTTP/2/3 chunks.
- `libc`: `epoll`, `eventfd`, socket options, `sendmsg`/`recvmsg`.
- `rustls` (feature `tls`, later): TLS 1.3/1.2, ALPN.
- `quinn-proto` + `rustls` (feature `h3`, later): QUIC transport only.
- `criterion` (dev only); no `http`, `httparse`, `h2`, `hyper`, `mio`, or `tokio`.

Edition 2024. License MIT OR Apache-2.0. README: philosophy, limits, guarantees, and
measured numbers.

## 12. Milestones

**M0 — Scaffold.** Crate, libringbuf path dependency, CI (fmt, clippy, test), benches,
license, fuzz targets skeleton.

**M1 — Reactor and buffers.** `epoll` loop, slab/tokens, timer heap, `eventfd`
commands, `TcpTransport`, `ReadBuffer`/`WriteBuffer` over libringbuf, accept session,
10k idle connection test.

**M2 — HTTP/1.1 engine.** Non-blocking parser/serializer, events, framing rules, limits,
chunked, trailers, 100-continue, upgrade/CONNECT detach, fuzz; in-memory transport
tests.

**M3 — HTTP/1.1 server.** Worker pool, body/response queues, dispatch, backpressure,
timeouts, graceful shutdown. Acceptance: curl/nginx interop, 10k idle, zero allocations
per request in the worker path, slow-header and slow-body tests.

**M4 — HTTP/1.1 client.** Reactor-driven pool, streaming responses, cancellation,
stale-connection retry.

**M5 — HTTP/2 engine.** Frames, stream states, HPACK, flow control, settings, errors;
`h2spec` later with the server.

**M6 — HTTP/2 server and client.** Per-stream jobs, responder channels, credit,
graceful shutdown, h2c prior knowledge. Acceptance: `h2spec`/`h2load` clean, 10k
concurrent streams, flow-control stress, no starvation with a slow consumer.

**M7 — Proxy.** h1↔h1, h1↔h2, h2↔h2, CONNECT/`101` tunnels; integration tests. This
closes the low-level API.

**M8 — TLS.** rustls `Transport` for HTTP/1.1 and HTTP/2, ALPN, certificate config,
`close_notify`, benchmarks and hardening.

**H0 — HTTP/3 spike (parallel, from M1).** quinn-proto loopback through our reactor,
h3 frame codec, QPACK static-table decoder, fuzz and a short report.

**H1 — HTTP/3 engine and endpoint (after M8).** QPACK dynamic table, control/QPACK
streams, request mapping, server/client integration, interop and benchmarks.

HTTP/3 production is after M8; the direction is fixed now, the spike starts early.

## 13. What is left to the builder

- Ring capacities, buffer pool sizes, shrink/grow policy, and arena sizing.
- Bounded queue/channel implementation (preallocated, no per-message allocation) and
  wake batching.
- Worker pool sizing and dispatch/queue policies.
- Reactor fairness budget and timer granularity.
- Interned header set and header index layout.
- HPACK/QPACK encoder strategies; dynamic table memory management.
- QUIC spike scope, quinn-proto version, and how much of h3 ships in v1.
- TLS adapter details (cert loading, `close_notify`, ALPN failure policy).
- Test corpus curation, h2spec/h2load invocations, QUIC interop tooling.

## 14. Calls made in this document (check these first)

1. HTTP/1.1 and HTTP/2 in v1; HTTP/3 direction fixed now, production after TLS, spike
   early.
2. One reactor, non-blocking I/O only; no thread-per-connection, no blocking socket
   path, no async.
3. Reactor owns every fd; workers never touch a socket; handlers run on a bounded pool.
4. `epoll` directly, not mio; mio has no blocking sockets anyway. Revisit portability
   later.
5. `libringbuf` for write buffers and HTTP/1.1 reads; `BytesMut`/`Bytes` for HTTP/2
   reads because chunks must outlive the buffer and cross threads.
6. HTTP/1.1 worker body chunks take one extra copy; the low-level/proxy path does not.
7. Typed, protocol-neutral message model; h2/h3 pseudo-headers become an absolute
   target; no synthetic `Host`.
8. The public HTTP/2 worker surface is `Responder`, `ResponseWriter`, `DataStream`,
   `DataChunk`; stream credit on drop, connection credit against a bounded buffer.
9. Cancellation/backpressure policy: refuse new streams, bounded queues, write
   deadlines, dropped responders reset.
10. `Session` is a trait object in the reactor; engines are concrete; no per-byte dyn.
11. Single crate, feature-gated modules; dependencies `libringbuf`, `bytes`, `libc`;
    `rustls` and `quinn-proto` later.
12. QUIC transport via quinn-proto; h3 framing and QPACK ours; quiche rejected for v1.
13. HTTP/2 push and priority omitted; extended CONNECT out of v1; padding accepted.
14. TLS after the proxy.
15. The server body cap is optional policy (`None` = unlimited), not a framing limit.
