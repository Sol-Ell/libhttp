# libhttp — Runtime Design (Transport, Buffers, TLS, Timers, Sockets, Close)

**Status:** part of the design for approval. **Date:** 2026-10-26.
**Parent:** [2026-10-26-libhttp-sync-http-design.md](2026-10-26-libhttp-sync-http-design.md).
This document binds the implementation. The parent settles scope, the public API, the §10 decisions,
performance gates, features and milestones; this document settles the runtime contracts beneath
`Server` and `Client`. No archived design was read.

---

## Runtime ownership

`Runtime` is the execution owner shared by `Server` and `Client`. It owns the reactor set, worker
pool, resolver threads, block/timer pools and admission counters. `Server` and `Client` are
front-ends: they add listeners, handler dispatch, the client pool and protocol configuration, but no
threads of their own. `Server::builder()`/`Client::builder()` create a private runtime; a shared one
is built with `Runtime::builder()` and used through `rt.server()`/`rt.client()`.

Reactor threads (`libhttp-reactor-N`) are created at runtime build. Worker threads
(`libhttp-worker-N`) start lazily on the first server or the first `fetch`/`fetch_with` callback;
resolver threads (`libhttp-resolver-N`) start on the first client. A client-only runtime using only
`send`/`send_with` has no workers; a server-only runtime has no resolvers. A
component shutdown stops only its connections/listeners and leaves the runtime usable;
`RuntimeHandle::shutdown` is the root and cancels both components. Failure isolation is a second
runtime, not a hidden per-component pool.

The same reactor hosts server and client connections; role and protocol live on the connection.
Client `send`/`send_with` block the calling thread and consume no reactor or library worker of their
own. `fetch`/`fetch_with` return after enqueue and use a runtime worker only for the upload/callback
job. If a blocking verb is called on a handler worker, that worker is blocked; the documented
`Request::defer` pattern frees it and does the blocking call on the application thread.

## Runtime public API

```rust
pub struct Runtime;                                        // Clone + Send + Sync
impl Runtime {
    pub fn builder() -> RuntimeBuilder;
    pub fn server(&self) -> ServerBuilder;                 // attaches, does not own
    pub fn client(&self) -> ClientBuilder;                 // attaches, does not own
    pub fn handle(&self) -> RuntimeHandle;
}

impl RuntimeBuilder {
    pub fn reactors(self, n: usize) -> Self;               // default min(available_parallelism(), 4)
    pub fn workers(self, n: usize) -> Self;                // default available_parallelism()
    pub fn resolver_threads(self, n: usize) -> Self;       // default 1
    pub fn max_client_callbacks(self, n: usize) -> Self;   // default max(1, workers / 2)
    pub fn build(self) -> Result<Runtime, Error>;
}

impl RuntimeHandle {
    pub fn shutdown(&self);
    pub fn shutdown_with_deadline(&self, grace: Duration);
    pub fn wait(&self) -> Result<(), Error>;
    pub fn stats(&self) -> RuntimeStats;
}

impl ServerHandle {
    pub fn runtime(&self) -> Runtime;
}
impl ClientHandle { pub fn runtime(&self) -> Runtime; }
```

- `Runtime::server()`/`client()` return builders attached to this runtime. `Server::builder()`/
  `Client::builder()` create a private runtime.
- `RuntimeBuilder::max_client_callbacks` is the global cap on `fetch`/`fetch_with` callback jobs and
  `fetch_with` upload jobs. It is separate from handler admission; exhausting it returns
  `Overloaded` from the verb.
- Reactors start at build; workers start with the first server or `fetch`/`fetch_with`; resolvers
  start with the first client. A client-only runtime using only blocking verbs has no worker threads;
  a server-only runtime has no resolver threads. `workers(0)` allows blocking-only use but makes
  `fetch`/`fetch_with` return `InvalidConfig`/`Overloaded` rather than spawning a worker lazily.
- `RuntimeHandle` is the root shutdown. `ServerHandle::shutdown`/`ClientHandle::shutdown` stop only
  their component. `RuntimeHandle::stats` is a relaxed snapshot; it does not expose per-operation
  costs.
- `Runtime` is refcounted; components and in-flight calls hold leases, so a late completion cannot
  touch a freed runtime.

---

---

## 1. Reactor loop

One reactor thread per reactor. Each iteration:

1. Compute the wait from the earliest timer; poll the native backend (`epoll_wait`/`kevent`/
   `GetQueuedCompletionStatusEx`).
2. Drain native events and completions; mark connections ready.
3. Run expired timers in deadline order; skip stale generation entries.
4. Drain cross-thread registration, buffer-return and deferred-completion queues; return blocks to
   the owning pool and wake detached exchanges that the application has completed or written to.
5. For each ready connection: feed transport bytes to TLS and the engine, let the engine produce
   output, write it, dispatch completed exchanges.
6. Dispatch ready exchanges to workers within admission limits (§ execution design).

No step calls user code. No step performs a syscall that can block. Every connection mutation happens
on its owning reactor; the only cross-thread entries are registration, buffer returns, and exchange
queues, all of which the reactor drains without waiting.

One timer heap per reactor is the single timer authority (§5). One buffer pool per reactor. One
socket slab per reactor. A reactor also owns the bounded detached-exchange table: a deferred exchange
keeps its connection slot, body/response queues and deadlines, but consumes no worker; application
completion or body writes enter through the same coalesced wake as worker output. Client connections
run the same loop and reactors with client-role engines; there are no separate client reactors.

---

## 2. Transport seam

Engines are sans-I/O and see a byte-flow contract, never a completion/readiness contract:

- `on_bytes(&[u8])` — plaintext bytes available (after TLS for TCP; QUIC stream bytes for HTTP/3).
- `can_send()` / output credit — the transport has room for more outbound plaintext.
- `on_eof()`, `on_error(io::Error)`, `on_timer(deadline, generation)`.
- Engine output goes into the connection's bounded outbound queue; the transport encrypts (TLS) and
  writes it. Engines never hold a socket, a buffer pool, or a timer.

### 2.1 Readiness backends (Linux epoll, macOS/BSD kqueue)

Sockets are nonblocking. Registration is edge-style (`EPOLLET` / `EV_CLEAR`) and every readable event
drains to `EAGAIN`; a spurious event must not stall a connection. Read interest is armed only while
reading is allowed; pausing means clearing read interest (`EPOLL_CTL_MOD` without `EPOLLIN`, or
disabling the kqueue read filter). Write interest is armed only while the outbound queue is above its
low-water mark. At most one block is being filled per connection. There is no outstanding kernel
operation, so close needs no cancellation wait.

`epoll_pwait2` supplies nanosecond timeouts where available; kqueue uses `EVFILT_TIMER` or the
`kevent` timespec, with kernel-tick resolution. Reactor wakeup is an eventfd (Linux) or a pipe
(macOS/BSD; `EVFILT_USER` is not relied on because of long-standing macOS bugs).

### 2.2 Completion backend (Windows IOCP)

Exactly one overlapped `WSARecv` is posted while reading is allowed, into the connection's current
block; exactly one `WSASend` is posted while output is pending. A completion with zero bytes is EOF;
an error is terminal. Pausing means not reposting. `AcceptEx` accepts are pre-posted in a bounded
pool with pre-created unbound sockets, `SO_UPDATE_ACCEPT_CONTEXT` is applied after each completion, and
the accept is reposted. `ConnectEx` is used with a prior `bind` and `SO_UPDATE_CONNECT_CONTEXT`.
`CancelIoEx` marks outstanding operations; each still completes (normally or with
`ERROR_OPERATION_ABORTED`), and the buffer/`OVERLAPPED` is not reused or freed until that completion
arrives. A connection slot is freed only after all posted operations have completed. Reactor wakeup is
`PostQueuedCompletionStatus` with a sentinel overlapped.

### 2.3 The shared arming contract

| Contract | Readiness backend | Completion backend |
|---|---|---|
| read in flight | at most one block being filled; interest armed once | exactly one posted recv |
| pause reading | clear read interest | do not repost recv |
| resume reading | set read interest | repost recv |
| terminal read | `read` returns 0 or error | completion with 0 bytes or error |
| write in flight | no kernel op; queue advances on writable | exactly one posted send |
| cancel read/write | clear interest; buffer reusable immediately | `CancelIoEx`; wait for the aborted completion |
| close | clear interest, `close` | cancel, wait for completions, `close` |
| op identity | `(slot, generation)`; slot reusable after generation bump | same; slot freed only after all completions |

A socket slot is reused only after its generation is bumped and no completion can still target it.
Late completions are matched against the generation and discarded.

### 2.4 Accept and connect

Listeners are bound in `Server::build`. The portable default is one acceptor reactor that accepts in
a loop (drain to `EAGAIN`, capped per wake) and hands accepted sockets to target reactors through a
registration queue bounded by `max_pending_accepts` (1,024); when it is full, accepts pause. The
target reactor performs epoll/kqueue registration, or on Windows associates
the socket with its IOCP (`CreateIoCompletionPort`) before posting the first read/write. Linux
`SO_REUSEPORT` per-reactor listeners are an optional gated fast path; macOS's `SO_REUSEPORT` does not
load-balance, so it is not used. FreeBSD `SO_REUSEPORT_LB` is phase 2.

Client connects use nonblocking connect plus writable/`SO_ERROR` on readiness backends and `ConnectEx`
on Windows; the connect timeout is a reactor timer that cancels the operation.

### 2.5 Datagram transport (QUIC)

One UDP socket per bound address per h3 reactor where `SO_REUSEPORT` is available (Linux/FreeBSD);
with several addresses there are several sockets. On macOS/Windows a single h3 reactor owns **all**
UDP sockets and all QUIC connections. Linux uses `recvmmsg`/`sendmmsg` (without the
buggy `recvmmsg` timeout argument) and optionally `UDP_SEGMENT`/`UDP_GRO`; macOS has no `recvmmsg` and
uses one `recvmsg`; Windows uses overlapped `WSARecvFrom`/`WSASendTo`. Each datagram is delivered to
`quinn-proto` as an owned `BytesMut` with remote address, local IP and ECN. Datagrams are received
directly into the `BytesMut` that quinn consumes; outgoing packets are encoded by
`Connection::poll_transmit(now, max_datagrams, &mut buf)` into a caller buffer and sent, with
`segment_size` driving GSO where the socket supports it.

`quinn-udp` is not used in v1: its socket abstraction and GRO stride handling are useful but would
introduce a second owner of the UDP socket while our transport seam already covers it. It remains a
phase-2 option if Linux GRO batching proves to matter in the h3 gate.

### 2.6 io_uring (phase 2)

Not a v1 backend. When added it is a completion transport behind the same seam: initially one-shot
recv/send/accept; later multishot recv/accept and provided buffers, with a per-socket group/share (a
shared ring cannot enforce a per-connection budget), an explicit `-ENOBUFS` policy, explicit
cancellation (closing an fd does not cancel), `IORING_FEAT_NODROP` or a fatal CQ-overflow policy, and
a runtime probe (`io_uring_setup` + `IORING_REGISTER_PROBE`) that falls back to epoll on `EPERM`,
`ENOSYS`, seccomp denial, or `kernel.io_uring_disabled`. The kernel ABI is not stable; a pinned,
maintained wrapper is used rather than raw syscalls. `SEND_ZC` stays out until its two-completion
protocol, partial-send fallback, notification-after-close, and iovec limits are specified here.

---

## 3. TLS

`rustls` (0.23.x, pinned 0.23.46) is the TLS implementation for TCP; QUIC uses the same rustls
installation through `quinn-proto`'s QUIC TLS wrapper. rustls is chosen because it is sans-I/O,
audited, and lets a reactor drive handshakes without a second thread. It is used **only if it meets
the TLS gate** (§7): if TLS throughput or latency regresses beyond the gate against hyper + rustls on
the same host, the cause is investigated before any further TLS work.

- **Provider:** `ring` only in v1. rustls and `quinn-proto` are configured with `default-features =
  false` so exactly one crypto provider is compiled. `aws-lc-rs`, FIPS, and other providers are phase
  2; adding them must not make `cargo build --all-features` ambiguous.
- **Driving:** ciphertext in through `read_tls` (rustls copies into its deframer),
  `process_new_packets` on the reactor, plaintext out through `reader()` into a plaintext block.
  Output through `writer()` and `write_tls` into ciphertext blocks. Handshakes are nonblocking and
  covered by `Timeouts::tls_handshake`; there is no handshake thread.
- **Plaintext ownership:** rustls owns and reuses its deframer buffer, so a borrowed
  `Reader::into_first_chunk()` slice cannot be handed to a worker; plaintext is copied into a block.
  That is why TLS costs two copies per direction (§4.5), stated rather than hidden.
- **`close_notify`:** sent after outbound ciphertext is flushed and before `shutdown(SHUT_WR)`.
  Received on the peer side is detected through `IoState::peer_has_closed()` (rustls 0.23.46 has no
  `received_close_notify()`); EOF without it is a truncation error. There is no public "ignore
  truncation" switch in v1.
- **ALPN:** TCP advertises `h2` and `http/1.1` according to enabled features and client/server
  configuration; QUIC advertises `h3`. Mismatch is a handshake error, never a silent fallback.
- **Certificates:** server certificates from PEM; client certificates and client certificate
  verification; client roots default to `webpki-roots`, with PEM roots configurable. rustls types are
  not exposed in the v1 public API; a custom verifier/resumption bridge is phase 2.
- **Not in v1:** SNI certificate switching, session-ticket rotation policy, native platform verifier,
  0-RTT, kTLS, TLS key logging.

---

## 4. Buffers

### 4.1 Blocks

Fixed 16 KiB refcounted blocks from a per-reactor pool. The pool grows lazily to a derived cap of
`max_buffered_bytes / max(1, reactors) / 16 KiB` blocks (256 MiB across four reactors ≈ 16,384
blocks total, 4,096 per reactor) and never allocates without bound. An exhausted pool stops reading
and triggers the global low-water reaction (§4.3) rather than allocating more. A block has a
refcount, a length, and an owning pool id. Splits are ranges, not copies.

A read may contain a body tail and the next request head, or DATA for several HTTP/2 streams, or
several h3 stream chunks. Ranges are shared; the refcount is incremented once when a block is first
shared with another thread/stream and decremented on the last drop. It is not per byte, per header, or
per frame. Inside the reactor, the parser and engine use the unique-owner path with no atomics.

### 4.2 Head storage and `HeaderMap`

The reactor parses and validates the raw head (framing fields, name/value syntax, limits). Typed
header decoding runs **when the exchange is handed to the handler or caller**, on the worker or on
the client's `send`/`send_with` thread or the `fetch`/`fetch_with` callback worker, not on the
reactor; a queued exchange
therefore holds only its raw head. This keeps
the reactor's work bounded and keeps typed-header allocation off the I/O path.

`Request`/`ClientResponse` own one refcounted head block plus a `HeaderMap`. The map is an ordered
list of two entry kinds:

- typed: `(HeaderName, TypeId, arena handle)`; the value lives once in a per-head typed arena;
- raw: a name plus a span into the head block owned by the map (the block is kept by an `Arc`), or
  `(HeaderName, HeaderValue)` for outgoing raw fields. `HeaderRef<'_>` is reconstructed per access,
  never stored alongside the block it borrows.

`HeaderRef<'a>` is the borrowed decoded input view; `HeaderValue` is the owned encoded output.
`Header::decode` takes one `HeaderRef` and is called once per field line; `Header::encode` returns one
`HeaderValue` or a typed error. Typed values are `Send + Sync` (`Header` requires it), so the map can
move with a deferred handle. There is no enum whose size is the largest header type, and users never match entry
variants: `get::<T>()` or the generated inherent accessor for typed headers, `get_raw`/`iter_raw` for
raw ones.

- HTTP/1.1 headers are parsed in place in the read block. A head that spans blocks is assembled into
  a bounded head buffer (one copy, ≤ `max_head_bytes`); body bytes remain in blocks.
- HTTP/2 and HTTP/3 field sections are materialized once into a bounded head buffer because HPACK/
  QPACK decode can span frames and reference table state. The bound is
  `h2_max_header_list_bytes` / `h3_max_header_list_bytes` (default 64 KiB each), plus the QPACK
  cumulative budget; HTTP/1.1's bound is `max_head_bytes` (default 16 KiB).
- Typed values are owned (`Header: Send + Sync + 'static`); variable-size values may allocate into
  the per-head arena, bounded by `max_header_arena_bytes` (64 KiB). `HeaderValue`'s inline storage
  absorbs short values. Raw custom fields stay zero-copy spans. The raw path remains allocation-free
  in steady state; typed-header allocations are counted and reported, not hidden.
- Standard header types use the generated known-name decoder table and inherent accessors;
  application types use the same `Header` trait through `register_header::<T>()` and a type-id
  lookup. Type erasure needs drop and encode function pointers for typed values, so each typed
  header pays one indirect call on decode, encode and drop; it is never per byte or per lookup.
  Standard accessors and decode entries are generated; extension values use the trait.
- Response typed headers are encoded to `HeaderValue`s on the worker (or the application thread for a
  deferred response) when the head is committed; the reactor only frames/HPACK/QPACK-encodes raw
  values. Typed-header `encode` never runs on a reactor. An `encode` error is a `500`/reset; a panic
  in an application `encode`/`decode` is caught at the boundary and mapped the same way.
- A deferred exchange moves the same request head/body and response state into `DeferredRequest`/
  `DeferredResponse`; the typed map and arena move with them. The application thread's body reads and
  response writes use the same bounded rings, deadlines and `Canceled` propagation as a handler; the
  reactor sees only enqueued data and completion.

### 4.3 Retention, quotas and memory arithmetic

Two explicit quotas bound memory; the per-connection one prevents one peer from eating the reactor's
pool, and the global one prevents a reactor from eating the process:

- `Limits::max_connection_buffer_bytes` (default 8 MiB for h2/h3, 256 KiB for h1) covers the
  connection's read block, inbound exchange queues and outbound queue. The per-exchange
  `max_request_body_buffer`/`max_response_body_buffer` values (64 KiB each) are **ceilings**, not
  simultaneous per-stream guarantees: a stream's effective **total** buffer is
  `min(max_request_body_buffer + max_response_body_buffer, connection_budget / max(open_exchanges, 1))`,
  where `open_exchanges` includes detached exchanges because they still hold streams and buffers,
  split between directions by demand, and unused shares may be borrowed while the total stays under
  the connection budget. With the defaults (32 streams, 8 MiB) the 128 KiB sum fits and every stream
  can use its ceilings; a user-raised stream count triggers sharing rather than immediate resets.
  A connection that exceeds its budget resets the stream that is over its share with no progress
  (h2/h3), or closes (h1). This is a safety net, not the normal path.
- `Limits::max_buffered_bytes` (default 256 MiB per server) counts all outstanding blocks **and
  typed-header arena bytes**, including blocks/arenas held by workers or detached exchanges. On reaching it the reactor resets the connections holding the most
  buffered bytes (those over their per-connection share) until usage falls below a 90% low-water
  mark; if it is still full, it stops reading and accepting, applies the normal read/write deadlines,
  and resets the top consumers again after `read_stall`. It never converts one reactor's pressure
  into an indefinite global stall.
- A queued exchange holds only its head, never a body: the reactor stops reading body bytes until a
  worker is assigned. That keeps `max_queued_exchanges` (default workers × 256) from multiplying
  `max_request_body_buffer`.
- A detached exchange holds its head, body/response rings and typed arena under the same
  per-connection and global buffer caps; `max_deferred_exchanges` (4,096) bounds the count, not the
  bytes. It counts as open for the per-connection buffer share. Application-thread body reads/writes
  do not take a library worker parking reservation; they are bounded by the connection/global caps
  and by `read_stall`/`write_stall`/`deferred`, and never affect worker parking.
- Idle keep-alive connections hold no blocks. Their slab entry, connection state and timer entry are
  targeted at ≤ 1 KiB per connection (plaintext); TLS connections retain rustls state and are
  measured separately. 10,000 idle plaintext connections per reactor therefore fit the gate without
  blocks; they are not "16 KiB per connection".
- Typed-header arenas exist for exchanges held by a worker, caller or detached table, not for queued
  raw heads. Each arena is capped at `max_header_arena_bytes` (64 KiB) and its bytes are charged to
  the connection and server-wide `max_buffered_bytes`; 4,096 detached exchanges therefore cannot
  allocate 256 MiB of arenas outside the cap. Exceeding the per-head or global cap is `431` (h1) or a
  stream/connection error (h2/h3), and `Request::defer` fails with `Overloaded` if the global
  reservation cannot be made.
- `max_connections` (default 100,000), `max_connections_per_reactor` (50,000) and
  `max_pending_accepts` (1,024 acceptor→reactor registrations) bound the slab and the handoff queue
  independently of buffers; accept pauses at the cap or when the registration queue is full.

### 4.4 `BodyChunk` ownership and shutdown

`BodyChunk` is `Send + Sync`, immutable, and holds a block reference. A handler may retain chunks; it
cannot free them early or forge them. Retaining all blocks delays that reactor's pool and therefore
its reads — bounded by `max_buffered_bytes`, not a leak. If the global cap stays at its low-water
mark, the reactor resets the connections holding the most buffered bytes (§4.3); a handler that keeps
chunks cannot be forced to drop them, but its connection is cut, its I/O returns `Canceled`, and the
memory is bounded by the global cap until the chunk drops. Shutdown does **not** wait for user-held
chunks: the reactor closes, the pool is marked dead, and the last drop of a chunk frees its block
directly instead of pushing to a dead return queue. Late drops from any thread are safe. This is the
"buffer accounting on close must not wait for user-held buffers" rule.

### 4.5 Copy and syscall budget

| Path | User-space copies | Allocations | Atomics |
|---|---|---|---|
| h1 plaintext request | 1 (kernel → block); 0 parse-in-place; head spanning blocks +1 | 0 on the raw path; typed-header values may allocate (arena ≤ 64 KiB) | 1 inc/dec per shared block |
| h1 static response | 0 for the body (`writev` from the static slice); head assembled in the outbound block | 0 steady state | 0 |
| h1 streamed response | 1 (handler bytes → outbound block) | 0 steady state | 0 |
| TLS read | 2 (ciphertext → deframer, plaintext → block) | 0 steady state | same as plaintext |
| TLS write | 2 (into rustls writer, out of `write_tls`) | 0 steady state | same as plaintext |
| h2/h3 headers | 1 materialization per field section, plus dynamic-table insertions | dynamic-table entries allocate; typed-header values may allocate into the arena | per shared block |
| QUIC inbound | 1 (kernel → `BytesMut`); quinn splits into refcounted `Bytes` without payload copy | one `BytesMut` per datagram | quinn's `Bytes` refcounts |
| QUIC outbound | 0 extra (`poll_transmit` writes into the caller buffer); kernel copy | 0 steady state | 0 |

Cross-thread handoff per plaintext HTTP/1.1 request with a static body:

- 1 read syscall, 1 write syscall, amortized event-loop waits (the "~4 network syscalls" figure);
- 1 futex wake (dispatch) + 1 futex wait (worker) + 1 condition-variable wake for the worker's
  wait on output capacity + 1 coalesced eventfd/IOCP wake for worker→reactor output (2–3
  synchronization syscalls, fewer when uncontended);
- 2 refcount atomics, one queue mutex lock/unlock per handoff.

The benchmark harness counts futex/eventfd as well as network syscalls; G1's ≤1.1× hyper applies to
the total, not just `read`/`write`. A "2 wakeups" claim is not made; the actual number is measured.
Allocations are counted separately for the raw path and for typed headers; a zero-allocation claim is
made only where the header values fit inline and no typed variable-size value is materialized.

---

## 5. Timers and deadlines

One timer heap per reactor is the **only** timer authority for connection and exchange deadlines.
Entries are `(deadline, slot, generation, exchange_seq, kind)`; an update pushes a new entry and stale
entries are skipped by comparing **both** generation and exchange sequence. The exchange sequence is
required because a keep-alive h1 slot can host exchange N+1 with the same generation; without it a
stale `read_stall`/`body_idle`/`total_request` entry from exchange N would cancel exchange N+1. The
fired entry's recorded deadline is checked before acting. Enforced deadlines: header read, body idle,
per-read stall, write stall, keep-alive, TLS handshake, graceful shutdown, handler I/O deadlines, and
QUIC PTO/keep-alive from `quinn-proto::Connection::poll_timeout`.

A worker's blocking read/write waits on the exchange condition variable **without its own timeout**.
When a deadline expires, the reactor records the terminal outcome (`Timeout(kind)` or `Canceled`) in
the exchange under its lock, wakes the worker, and the worker returns that outcome. The worker never
invents a timeout for an exchange the reactor still considers live; there is no second timer
authority and no race at the deadline instant.

The client call queue is reactor-owned too: the client verbs enqueue a call and
`PoolConfig::queue_timeout` is a normal reactor deadline. The blocking caller or the callback waits;
it does not own connection deadlines. Once a call is matched to a connection, the reactor owns its
deadlines.

Timer resolution is the backend's: millisecond on epoll/IOCP, kernel-tick on kqueue. Sub-millisecond
precision is not promised. QUIC's 1 ms timer granularity is respected but not exceeded.

---

## 6. Socket ownership, listeners and close

- A reactor owns a slab of connection slots; a connection is `(slot, generation)`. All socket
  operations happen on the owning reactor. Public types contain no socket or handle.
- Accepted sockets are handed to the target reactor through a registration queue; the target
  registers with epoll/kqueue or associates with its IOCP, then starts I/O. On Windows,
  `AcceptEx`'s `SO_UPDATE_ACCEPT_CONTEXT` is applied before handoff.
- Listener options: `SO_REUSEADDR` on Unix; no `SO_REUSEPORT` by default (Linux gated fast path);
  `TCP_NODELAY` on accepted/client sockets by default (configurable); keepalive off by default.
  Addresses are taken as given: binding `0.0.0.0` is IPv4-only, `[::]` is IPv6 (with platform
  dual-stack behavior left to the OS); the server does not silently bind both.
- **Graceful close order:** stop reading new requests → finish/abort in-flight exchanges per the
  shutdown policy → flush outbound → TLS `close_notify` → `shutdown(SHUT_WR)`/`SD_SEND` → optionally
  read until peer FIN or a short timeout (bounded, to avoid an RST that discards the last response) →
  cancel outstanding reads → close → return blocks → bump generation and free the slot. Errors skip
  the graceful steps and go straight to cancel/close.
- **Windows:** the slot and buffers live until every posted operation completes
  (`ERROR_OPERATION_ABORTED` or normal); the generation guards late completions. Readiness backends
  have no outstanding operations, so close is immediate.
- **Half-close** exists internally for TLS `close_notify` and HTTP/1.1 early responses:
  `shutdown(SHUT_WR)`/`SD_SEND`; `SIGPIPE` is suppressed with `MSG_NOSIGNAL` (Linux) and
  `SO_NOSIGPIPE` (macOS/BSD); `EPIPE`/`WSAESHUTDOWN` map to `Canceled`/`Closed`, never panic. There is
  no public tunnel, CONNECT, or `101` API in v1; such requests are rejected.
- **User-held blocks are not waited for** during close (§4.4); the socket slot is freed independently
  of chunk lifetime.
- **Deferred exchanges** keep the connection slot and its rings until the application completes or
  the deadline/shutdown cancels them. An h1 connection with a deferred exchange accepts no next
  request; h2/h3 continue other streams. Completion from the application thread wakes the reactor
  through the coalesced wake; the reactor performs the write/close. Cancellation unlinks the exchange
  and marks its shared state terminal; handles keep an `Arc` to that terminal state, so a blocked
  application read/write wakes with `Canceled` and late `send` fails without touching freed rings.
  Handles may outlive `Server::run` and are simply invalid; dropping them frees the last reference.
  Dropping a handle without completing follows the deferred drop rules: `DeferredResponse` sends
  `500` if the head is uncommitted and the connection permits, otherwise resets/closes;
  `DeferredRequest` with an unread body applies the drain/close/reset rule.

---

## 7. TLS performance gate

TLS is not exempt from the "does not slow things down" condition. At M3, on the same fixed host and
core budget as G1, with the same hyper + rustls comparison server:

- TLS throughput ≥ 0.90× hyper + rustls, and p99 ≤ 1.25× hyper + rustls, for a 15-byte body and
  keep-alive.
- TLS overhead versus our own plaintext path is published (not gated): handshake cost, per-request
  bytes and copies, session resumption hit rate with the default in-memory caches.

If the TLS gate misses, the cause is profiled (record sizes, `write_tls` batching, handshake CPU,
provider) before changing the TLS stack; kTLS is phase 2. If rustls cannot meet the gate, that is a
scope decision for the project owner, not a silent substitution.

---

## 8. Portability matrix

| Capability | Linux | macOS | FreeBSD | Windows |
|---|---|---|---|---|
| TCP readiness | epoll (ET, `epoll_pwait2`) | kqueue (`EV_CLEAR`) | kqueue (`EV_CLEAR`) | — |
| TCP completion | io_uring (phase 2) | — | — | IOCP |
| Reactor wakeup | eventfd | pipe | pipe/kqueue | `PostQueuedCompletionStatus` |
| Accept distribution | acceptor + optional `SO_REUSEPORT` | acceptor (no LB reuseport) | acceptor + `SO_REUSEPORT_LB` phase 2 | acceptor + IOCP handoff |
| UDP batching | `recvmmsg`/`sendmmsg`, GSO/GRO | single datagram | `recvmmsg` | single datagram; USO phase 2 |
| QUIC reactors | one per reactor/address | one reactor owns all UDP sockets | one per reactor/address | one reactor owns all UDP sockets |
| Half-close | `SHUT_WR` | `SHUT_WR` | `SHUT_WR` | `SD_SEND` |
| `SIGPIPE` | `MSG_NOSIGNAL` | `SO_NOSIGPIPE` | `SO_NOSIGPIPE` | n/a |
| Sendfile/splice | phase 2 | phase 2 | phase 2 | phase 2 |

Runtime capability probes decide optional accelerators (io_uring, `SO_REUSEPORT`, `TCP_FASTOPEN`,
USO); absence degrades performance only. No public type or behavior differs by platform.

---

## 9. Fast-path scope

**v1 baseline:** native backends; edge-triggered readiness with full drain; one-shot completion I/O;
`writev`/`WSASend`; accept loops; buffer pooling; Date caching; `TCP_NODELAY`; Linux datagram batching.

**v1 optional, benchmark-gated:** Linux `SO_REUSEPORT`; `TCP_FASTOPEN`; `EPOLLEXCLUSIVE`;
`UDP_SEGMENT`/`UDP_GRO`. Each must beat the portable path on the published benchmark or stop being a
default; the code stays behind a capability check.

**Phase 2, behind the same seam:** io_uring (one-shot, then multishot/provided buffers/`SEND_ZC` with
the full protocol), `sendfile`/`splice`, `MSG_ZEROCOPY`, Windows USO/receive coalescing, IoRing
(Win11 22000+), FreeBSD `SO_REUSEPORT_LB`, kTLS, quinn-udp/GRO integration. No fast path changes
correctness or the public API.
