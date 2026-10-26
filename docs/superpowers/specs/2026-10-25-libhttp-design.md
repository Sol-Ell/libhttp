# libhttp — Design

**Status:** draft for approval (revision 3). Nothing is implemented yet.
**Date:** 2026-10-26.
**Scope:** HTTP/1.1, HTTP/2, and HTTP/3 in one non-blocking, completion-driven Rust
library for Linux, macOS/BSD, and Windows.

## 1. Intent

Rust's HTTP stack is framework-shaped: it assumes an async runtime, hides allocation
and copying behind trait objects, and makes the cost of a request hard to see or
predict. Migrating between runtimes is expensive because the runtime is woven through
the API, and `async`/`.await` makes control flow and cancellation harder to reason
about than the protocol requires. "No async" is not an excuse for thread-per-connection
and blocking sockets either: that trades the runtime's problems for scheduler, memory,
and syscall problems at exactly the connection counts HTTP/2 and HTTP/3 exist to serve.

`libhttp` is a performance-first, non-blocking library, not a framework. A native
completion driver owns every socket. Protocol engines are sans-io: bytes and state
transitions in, typed messages and queued buffers out. Application handlers run on a
bounded worker pool and never touch a socket. There is no async runtime, no `Future`,
no blocking socket I/O, and no thread per connection.

The one-sentence rule: **every abstraction on the hot path must pay for itself in
measured cost, and every copy, allocation, wakeup, or syscall must be visible in the
API or the documentation.**

### Abstraction and dependency stance

Traits, generics, and trait objects are fine when they buy performance, portability, or
a genuine architectural boundary. They are not fine when they force a structure on the
user or sit on the per-field or per-byte path. This design uses:

- `Session` for things the reactor drives — one virtual call per completion, not per
  byte.
- `Handler` for application code — one call per exchange.
- `HeaderCodec` for HPACK and QPACK — one call per field block; this abstraction exists
  because HTTP/3 needs QPACK and pays for itself immediately.
- concrete engines, concrete events, concrete buffers elsewhere.

It does not have a `Transport`, `Body`, `Stream`, `Service`, or middleware trait.
HTTP/1.1, HTTP/2, and HTTP/3 are concrete engines with different semantics; shared code
is data types, buffers, codecs, and the reactor.

Dependencies must earn their place on measured value:

- `io-uring` manages the Linux submission/completion queues; hand-rolling it would be
  worse code for no gain.
- `rustls` provides TLS without hand-rolled crypto.
- `quinn-proto` provides QUIC (loss recovery, congestion control, packet protection,
  TLS 1.3); HTTP/3 framing and QPACK remain ours.
- `libc` and `windows-sys` are the OS surface.
- `bytes` types never appear in our public API or in our buffer core; `quinn-proto`
  uses `bytes` internally, and the h3 adapter copies from its chunk type into our
  `Buf` at that boundary only.
- `libringbuf` is not used. It is the wrong shape for completion-based I/O and costs
  mmap VMAs; §6.3 shows why a pooled refcounted buffer is both simpler and faster here.
- `mio` is not used. It is readiness-only, and this design is completion-first on every
  platform.

### What "typed, not strings" means here

- `Method`, `StatusCode`, `Version`, and header names are typed; unknown or custom
  values are byte slices with explicit escape hatches, never `String`.
- A request target is parsed into its forms (`origin`, `absolute`, `authority`,
  `asterisk`) with path/query as borrowed bytes; no percent-decoding at this layer.
- Header lookup is a linear scan over interned names with a small inline index, not a
  hash map per message.
- Bodies are streaming handles with explicit framing, not `Vec<u8>` collected from the
  socket.
- Parsed messages borrow exchange-owned storage; crossing a thread boundary moves an
  arena or a refcounted buffer.

## 2. Goals

1. HTTP/1.1, HTTP/2, and HTTP/3 server and client, over TCP, TLS, and QUIC.
2. Completion-based, non-blocking I/O on Linux (io_uring), macOS/BSD (kqueue), and
   Windows (IOCP); no blocking socket call, no thread per connection, no async.
3. Public low-level driver/session/buffer types so a proxy, a protocol tester, or an
   embedded use can be built without forking.
4. A worker model for ordinary synchronous handlers that never stalls the reactor.
5. Typed, protocol-neutral messages and shared header-codec infrastructure that HTTP/3
   genuinely uses.
6. Correctness against the RFCs, request-smuggling defenses, fuzzed parsers.
7. Measured performance: 10k idle connections per reactor, bounded memory, published
   syscall/allocation/copy budgets on every supported platform.

## 3. Non-goals

- Async runtimes, `Future`, `Waker`, `Pin`, task spawning.
- Blocking socket I/O and thread-per-connection.
- A routing framework, middleware stack, extractors, or dependency injection.
- Caching (RFC 9111), content negotiation, compression, cookies, sessions.
- WebSocket framing; upgrades and `CONNECT` detach to an opaque byte tunnel.
- HTTP/2 server push, priority, and HTTP/3 push.
- Extended `CONNECT` (RFC 8441) in v1.
- `no_std`.
- Hand-written TLS or QUIC transport; both are delegated to `rustls` and
  `quinn-proto`, while HTTP/3 framing and QPACK are ours.
- A blocking or readiness-only path on Linux when io_uring is available.

## 4. Anti-goals

- An ergonomic API that hides a copy, an allocation, a wakeup, or a syscall.
- A blocking call on the I/O path, however convenient.
- A thread per connection, however simple.
- Readiness where completion is available and faster.
- A dependency that does not buy measured performance, portability, or correctness.
- A trait object per field or per byte where a concrete type, or one block-level call,
  would do. Headers are concrete types; traits are welcome at boundaries (reactor
  session, codec, handler), not per-field dispatch.
- An abstraction added only for a protocol that is not implemented yet. HTTP/3 is in
  scope now, so abstractions that pay for it (completion I/O, `Buf`, `HeaderCodec`) are
  in scope too.
- Benchmark claims without the harness, hardware, and versions beside them.

## 5. Constraints and decisions from the conversation

- HTTP/1.1, HTTP/2, and HTTP/3 are all required; HTTP/3 is not deferred.
- All major platforms: Linux, macOS/BSD, Windows.
- Platform-specific features are allowed; hiding the OS is not a goal.
- Dependencies must prove worth; "better" means faster where performance is the
  criterion.
- `libcommon` is not a dependency. `libringbuf` is not used. `bytes` is not part of our
  public API or buffer core; it exists only inside the `quinn-proto` h3 adapter.
- Low-level types are public; the proxy is the acid test.
- Handlers run on a worker pool. Stackful coroutines are rejected.
- Thread-per-connection and blocking I/O are rejected.
- `async`/`await` is rejected.
- QUIC transport: `quinn-proto`. Platform I/O: native backends, no mio.

## 6. Approaches considered

### 6.1 Message API shape

**A — transport mirror.** `HttpListener` yields an `HttpStream` owning a socket; it is
clean for HTTP/1.1, false for HTTP/2 and HTTP/3 (one connection, many streams, shared
HPACK/QPACK and flow control), and forces separate high-level models. Rejected.

**B — connection + exchange handles (chosen).** A `Connection`/engine is one protocol
state machine for one transport connection; a `Stream` is an exchange handle into it.
HTTP/1.1 has one implicit stream at a time, HTTP/2 many, HTTP/3 many over QUIC streams.
The high-level server/client share policy, not protocol structure.

### 6.2 I/O model

**Thread per connection, blocking I/O.** Simple, fast at low concurrency, wrong here:
threads and stacks per idle connection, blocked handlers stalling multiplexed streams,
blocking writes deadlocking against clients that are still sending, and no way to use
io_uring or IOCP well. Rejected.

**Readiness reactor (epoll/kqueue).** The conventional answer. It works, but it spends
syscalls on readiness notification and then on the actual operation, needs
drain-until-`WouldBlock` discipline, and re-arms interest for every state change. On
Linux it is strictly worse than io_uring when io_uring is available.

**Completion reactor (chosen).** Every operation is submitted with a stable buffer and
completes with a result: read this socket into this buffer, write this buffer, accept
this listener, connect here, wait until this instant. io_uring and IOCP implement this
natively; epoll and kqueue backends emulate completions by performing the operation
when readiness arrives. This is the performance-first model on every platform, and it
makes buffer ownership explicit instead of hiding it in `read`/`write` calls.

**All async.** Rejected: runtime coupling, `Future` machinery, and the problem this
library exists to avoid.

**Stackful coroutines.** Rejected: stacks, context switching, unsafe scaffolding, and a
scheduler the library does not otherwise need.

### 6.3 Buffer model

**Contiguous ring buffer (`libringbuf`).** A ring gives one readable slice and one
spare slice without wrap. That is useful for a persistent stream buffer, but this design
does not keep one: each completion carries its own stable buffer. A ring cannot hand
out stable chunks while it is refilled, so HTTP/2 DATA and worker handoff would need a
copy. It also costs a memfd and several VMAs per active buffer, which hurts 10k-
connection targets and portability.

**Refcounted byte chunks (`bytes`).** The right shape, but the user rejected its
implementation quality: undocumented unsafe and missed trivial optimizations. We are not
going to wrap it and inherit that.

**Chosen: pooled, refcounted `Buf`/`BufMut` owned by libhttp.** A `BufMut` is a unique
buffer taken from a per-reactor pool, handed to a read completion, and frozen into a
`Buf`. A `Buf` is an atomically refcounted allocation plus a `(start, end)` range;
splitting or slicing is O(1) and never copies. `Buf` is `Send`, so body chunks cross to
workers without a copy. `BufMut` serializes responses and is frozen for a write
completion. The last `Buf` drop returns the allocation to the pool.

Performance argument:

- One buffer per completion, no ring refill or wrap logic.
- Zero-copy split for HTTP/2 DATA, HTTP/1.1 body chunks, and worker handoff.
- No mmap, no memfd, no VMAs, no `vm.max_map_count` concern; works identically on all
  three platforms.
- Pool reuse makes the data allocation a pointer pop. `Arc` adds one small refcount-
  header allocation per frozen buffer; it is measured, and an intrusive refcount can
  replace it later without changing the `Buf` API.
- The only new unsafe is the read-into-spare slice, which is small, documented, tested
  under Miri, and fuzzed at the buffer level.

If profiling later shows a faster shape, the `Buf` API hides the representation; the
engines never depend on it being an allocation. But the ring is not that shape for
completion I/O, and `bytes` is not an acceptable dependency here.

### 6.4 QUIC backend

`quinn-proto` is chosen: deterministic, sans-io QUIC with no networking code and no OS
timestamps, designed for a foreign event loop. It gives loss recovery, congestion
control, packet protection, and a rustls TLS 1.3 handshake; we drive it from our UDP
completion path. HTTP/3 framing, QPACK, control streams, and request mapping are ours.

`quiche` was the alternative: high quality and no `bytes`, but it would pull
BoringSSL (a second TLS stack next to rustls) and a cmake/NASM build, and its h3 module
would tempt delegation of the part we want to own. Hand-written QUIC is a separate
project, not a milestone. `quinn-proto`'s internal use of `bytes` is isolated in the h3
adapter; our public API and our buffers never expose it.

Forking `quinn-proto` to replace `Bytes`/`BytesMut` with our `Buf` is rejected for v1.
Its buffer types are used in packet assembly, retransmission, and stream reassembly;
replacing them means maintaining a fork of a large, security-sensitive protocol stack
and reimplementing the `Buf`/`BufMut` semantics it relies on. The boundary copy is one
`memcpy` per received h3 chunk, measured against QUIC crypto and packet processing. If
it shows up in profiles, the order is: optimize the adapter's pooled copy, propose a
generic buffer boundary upstream, and fork only as a last resort with a measured win.
`bytes` trait definitions may be used if an interop need appears; its `Bytes`/`BytesMut`
implementations stay out of our core.

## 7. Architecture

```
              +-------------------------------------------------------+
  app ------> | Server / Client (policy, pooling, dispatch)           |
              +-------------------------------------------------------+
                                  |
              +-------------------------------------------------------+
              | h1 engine | h2 engine | h3 engine                     |  sans-io
              | framing, HPACK/QPACK, flow control, typed messages    |  no I/O
              +-------------------------------------------------------+
                    |                                      |
              +---------------------------+        +------------------+
              | Reactor + Session         | <----> | Worker pool      |
              | completions, timers,      |        | (handlers only)  |
              | worker command queue      |        |                  |
              +---------------------------+        +------------------+
                                  |
              +-------------------------------------------------------+
              | Driver: io_uring (Linux) | epoll fallback | kqueue     |
              |         | IOCP (Windows)                             |
              +-------------------------------------------------------+
                                  |
              +-------------------------------------------------------+
              | Sockets (TCP/UDP), Buf/BufMut pools, timer heap       |
              +-------------------------------------------------------+
```

The driver owns every socket. Sessions submit operations and receive completions. The
engines never perform I/O, never spawn threads, and never run handlers. Workers never
see a socket. That is the whole concurrency model.

### 7.1 Completion driver and reactor

```rust
pub struct Reactor { /* driver, slab, sessions, timer heap, command queue */ }
pub struct SocketId(u32);
pub struct ListenerId(u32);
pub struct OpId(u64);

pub enum Op {
    Accept { listener: ListenerId, id: OpId },
    Connect { addr: SocketAddr, id: OpId },
    Read { socket: SocketId, buf: BufMut, id: OpId },
    Write { socket: SocketId, bufs: WriteBufs, id: OpId },
    RecvFrom { socket: SocketId, buf: BufMut, id: OpId },
    SendTo { socket: SocketId, buf: Buf, addr: SocketAddr, id: OpId },
    Timeout { at: Instant, id: OpId },
}

pub enum Event {
    Accepted { id: OpId, socket: SocketId, peer: SocketAddr },
    Connected { id: OpId, socket: SocketId },
    Read { id: OpId, buf: Buf },                // len is the received length
    Written { id: OpId, bufs: WriteBufs, n: usize }, // bufs is the unwritten remainder
    Datagram { id: OpId, buf: Buf, from: SocketAddr },
    TimedOut { id: OpId },
    Closed { socket: SocketId },
    Failed { id: OpId, error: io::Error },
}

pub trait Session {
    fn sockets(&self) -> &[SocketId]; // ownership declared at registration
    fn on_event(&mut self, event: Event, io: &mut Io) -> Step;
}

pub enum Step {
    Continue,
    Register(Box<dyn Session>), // the new session's sockets() declare its socket
    Close,
}

impl Reactor {
    pub fn new(config: ReactorConfig) -> io::Result<Self>;
    pub fn run(self) -> io::Result<()>;
    pub fn step(&mut self, timeout: Option<Duration>) -> io::Result<()>; // tests
    pub fn listen(&mut self, addr: SocketAddr, backlog: u32) -> io::Result<ListenerId>;
    pub fn bind_udp(&mut self, addr: SocketAddr) -> io::Result<SocketId>;
    pub fn register(&mut self, session: Box<dyn Session>) -> SessionId;
    pub fn handle(&self) -> ReactorHandle; // wake(), shutdown()
}
```

`Io` is the session's only interface to the driver: `submit(Op)`, `close(socket)`,
`timeout(at, id)`, `now()`, `wake()`. A session can own several `SocketId`s — that is
what makes a proxy and a QUIC endpoint expressible in one session without fd-per-session
hacks. A session never receives `&mut Reactor`; session creation uses `Step::Register`,
which the reactor applies after the callback. One vtable call per completion is the
entire dynamic-dispatch cost.

At most one read and one write operation are in flight per stream socket. This is a
contract, not an implementation detail: multishot recv and provided-buffer rings are
explicitly out of v1 because they break one-buffer-per-completion and let the kernel
choose buffers. Batching of submissions/completions and multishot accept are in. A write
takes a `WriteBufs` chain, so a frame header and a zero-copy payload are one vectored
write; the session queues the next chain when the completion arrives. This preserves
write ordering, bounds buffers, and makes partial writes and backpressure explicit.
Datagram sockets may have several `SendTo` operations in flight because datagram order
is not part of the contract.

Buffer and close contract:

- The driver keeps every submitted `Buf`/`BufMut` alive until its completion and returns
  it exactly once. A session must not touch a submitted buffer.
- `Io::close(socket)` marks the socket closing, rejects new operations, and cancels or
  drains in-flight operations. Each pending buffer comes back through its completion;
  once the last one is accounted for, the driver emits `Event::Closed { socket }` and
  releases the fd/handle. `OpId`s may be reused only after their completion (or after
  `Closed` for cancelled operations).
- `Step::Close` closes every socket the session declared in `sockets()`, cancels or
  drains operations the same way, and delivers no further events to that session. The
  reactor drops the returned buffers only after cancellation is accounted for.
- An accepted or connected socket is handed to a new session through its constructor;
  the session returns it from `sockets()`, and the reactor registers the mapping. That
  is the handoff contract for `Step::Register`. A socket created by `Op::Connect` is
  mapped to the session that submitted the operation when the completion arrives; a
  session that later hands a socket to another session does so through `Register`.

Datagram operations (`RecvFrom`/`SendTo`) exist for QUIC and are first-class, not an
afterthought. `Timeout` is a completion, so timers use the same path as I/O and there is
no timer thread. An idle connection keeps one small read completion armed (4 KiB class)
so data wakes it without a readiness syscall; the completion returns the buffer with
data, which the engine consumes like any other read. That is the completion-native way
to wait, and it behaves the same on every platform.

- **io_uring (Linux):** single-shot accept, recv/send, connect, recvmsg/sendmsg,
  timeouts; batching of submissions and completions; multishot accept; registered files
  are an optimization behind the same `Op` API.
- **epoll fallback (Linux):** used only when io_uring cannot be initialized (older
  kernel, seccomp, container policy). Readiness emulates a completion by performing the
  requested operation; edge-triggered and drain-until-`WouldBlock`.
- **kqueue (macOS/BSD):** `EVFILT_READ`/`EVFILT_WRITE`, `EVFILT_USER` for wakeups, and
  the poll timeout for timers; readiness emulates completions.
- **IOCP (Windows):** overlapped `WSARecv`/`WSASend`/`AcceptEx`/`ConnectEx`,
  `GetQueuedCompletionStatusEx`, `PostQueuedCompletionStatus` for wakeups; native
  completions.

`ReactorConfig` chooses `Auto`, `IoUring`, or `Epoll` on Linux; `Auto` probes io_uring
at startup and falls back only on failure. No runtime choice is hidden from the user;
the selected backend is reported.

### 7.2 Platform layer

- Linux: raw sockets via `libc`, `epoll`/`eventfd`, `io-uring` crate for SQ/CQ.
- macOS/BSD: `libc` sockets and `kqueue`; `EVFILT_USER` for wake.
- Windows: `windows-sys` sockets, IOCP, overlapped I/O, `AcceptEx`; no readiness
  emulation on the primary path.
- Common `Socket` wrapper: non-blocking, `TCP_NODELAY`, keepalive, `SO_REUSEPORT` where
  available, `SO_REUSEADDR` semantics per OS, dual-stack behavior per OS.
- Cross-platform CI from M0: Linux, macOS, Windows. Functional tests on all; performance
  benchmarks on Linux and Windows; io_uring tests on a kernel that supports it, epoll
  fallback tests forced by config.

`SO_REUSEPORT` is Linux/BSD; Windows uses `SO_REUSEADDR` plus a single accept loop. No
platform-specific behavior is hidden behind a portability layer that changes semantics;
the differences are documented.

TLS is a session wrapper, not an engine concern. It submits reads into ciphertext `Buf`s,
decrypts into plaintext `Buf`s, feeds the h1/h2 engine, encrypts output, and submits
writes. rustls' non-blocking state machine fits the completion loop; handshake and
`close_notify` are ordinary events. ALPN selects `http/1.1` or `h2`. QUIC carries its own
TLS 1.3 handshake inside `quinn-proto`; both paths share the rustls configuration and
certificate loading.

### 7.3 Buffers

```rust
pub struct Buf { /* Arc<BufInner> + start/end */ }
pub struct BufMut { /* Vec<u8> + PoolHandle */ }
pub struct WriteBufs { /* inline 8 Bufs + overflow Vec */ }
pub struct Pool { /* size classes, free lists */ }
pub struct PoolHandle(/* Arc<Pool> */);

impl Buf {
    pub fn split_at(self, at: usize) -> (Buf, Buf);
    pub fn slice(&self, range: Range<usize>) -> Buf;
    pub fn advance(&mut self, n: usize);
    pub fn len(&self) -> usize;
}
impl Deref for Buf { type Target = [u8]; }
impl BufMut {
    pub fn capacity(&self) -> usize;
    pub fn freeze(self) -> Buf;      // publishes the initialized length
    pub fn extend_from_slice(&mut self, src: &[u8]);
}
impl WriteBufs {
    pub fn push(&mut self, buf: Buf);
    pub fn consume(&mut self, n: usize); // partial write: drop the written prefix
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
}
```

- A `Buf` is immutable and `Send`; cloning/splitting is an atomic refcount bump and an
  O(1) range change. The last drop returns the allocation to the pool. One small
  refcount-header allocation per frozen buffer is accepted and measured; replacing it
  with an intrusive refcount is an optimization, not an API change.
- A `BufMut` is uniquely owned, taken from the pool, and either frozen for writing or
  filled by a read completion. Read into uninitialized spare capacity is the only unsafe
  operation; it is encapsulated, documented, and Miri/fuzzed.
- `WriteBufs` is a small inline chain of immutable `Buf`s (8 inline, overflow into a
  `Vec`). Frame headers and response heads are small pooled `Buf`s (64-byte class)
  chained in front of zero-copy payload `Buf`s; the driver writes them with
  `IORING_OP_WRITEV`, `WSASend`, or `writev`. There is no all-output coalescing copy.
  A partial write consumes the written prefix and returns the remainder for resubmission.
- Pools are per reactor, with size classes (64 B, 1 KiB, 4, 16, 32, 64 KiB; larger
  bodies chain buffers). Workers return buffers through the pool's thread-safe free
  list; a lock per buffer is acceptable at these sizes and is measured.
- Idle connections keep one small (4 KiB class) read completion armed; the buffer is
  returned to the pool when the completion arrives or the connection closes. A write
  takes a buffer only when there is output. This is the completion-native wait on every
  platform. An empty pool is backpressure on accept/read submission, not a reason to
  allocate unbounded memory.
- Header arenas are separate, per exchange, and bounded by the header limit.

Copies and lifetimes:

- Read: kernel→`Buf` (one copy by the kernel), then zero-copy slices to the engine,
  worker, or response.
- Write: application bytes→`BufMut` once when they are produced; forwarded DATA reuses
  its existing `Buf`; kernel→wire once.
- HTTP/1.1 body chunks, HTTP/2 DATA, proxy forwarding, and worker handoff are zero-copy
  `Buf` slices. A frame header is a tiny `Buf` in the same `WriteBufs` chain.
- HTTP/3: `quinn-proto` hands us its chunk type; the adapter copies once into a `Buf`
  because our public types must not expose `bytes`.
- TLS: rustls copies between ciphertext and plaintext buffers; that is inherent to the
  TLS boundary and counted.
- Header bytes: HTTP/1.1 one bounded copy into the arena; HTTP/2/3 decoded once into
  the arena. Response header slices are copied once into the write buffer by the
  adapter.

### 7.4 Message model

- `HeaderName<'a>`: `Known(HeaderId)` for the standard set plus common extension names,
  `Custom(&'a [u8])` otherwise; comparison by ID, then case-insensitive bytes.
- `HeaderValue<'a>`: a newtype over the field-value bytes, with `as_bytes`, `Deref`, and
  typed accessors where the field has a defined grammar (`content_length()`,
  `content_type()`, ...). Field values are octets, not `str`; no UTF-8 validation.
- `Header<'a> { name: HeaderName<'a>, value: HeaderValue<'a> }` and `Headers`: ordered
  field lines preserved exactly, duplicates included, with a case-insensitive lookup
  index (default 32 inline slots). Nothing is merged into a map; `Set-Cookie` is never
  combined.
- Headers are types, not trait objects. `HeaderName`, `HeaderValue`, `Header`, and
  `Headers` are concrete; there is no `Box<dyn Header>`. A trait would have no
  behavioral variation to dispatch to, and per-field dispatch would cost more than the
  lookup. Traits appear at the field-section/codec boundary (`HeaderCodec`, one call per
  block) and at the reactor handler boundary, where they buy something.
- `Method`, `StatusCode`, `Version`, `RequestTarget<'a>` as before.
- `BodyLength { None, Fixed(u64), Chunked, UntilClose, Tunnel }` for HTTP/1.1;
  trailers are a separate end-of-body field set, never merged.
- `RequestHead` is protocol-neutral. HTTP/2 and HTTP/3 pseudo-headers become an
  `Absolute` target (`:scheme`, `:authority`, `:path`); no synthetic `Host` field is
  created, and `authority()`/`scheme()`/`path()` consult the target first and `Host`
  second.
- HTTP/2 and HTTP/3 encoding lowercases field names through a byte table without
  allocating and rejects connection-specific fields.
- Header storage is an arena filled once by the parser/HPACK/QPACK decoder; it moves
  with the exchange. Nothing in an `OwnedHead` points into a buffer the reactor will
  refill.

### 7.5 Header codecs

`HeaderCodec` is the abstraction that pays for HTTP/3:

```rust
pub enum Decode {
    Complete(FieldSection), // owns its arena
    Blocked,                // codec retains the block and its arena
}

pub trait HeaderCodec {
    fn decode(&mut self, stream: StreamId, block: FieldBlock) -> Result<Decode>;
    fn resume(&mut self, out: &mut Vec<(StreamId, FieldSection)>) -> Result<()>;
    fn encode(&mut self, section: &FieldSection, out: &mut WriteBufs) -> Result<()>;
    // QPACK control streams; default: unsupported (HPACK).
    fn control(&mut self, kind: ControlKind, buf: Buf) -> Result<()> { Err(Unsupported) }
    fn take_control_output(&mut self, out: &mut WriteBufs) {}
    fn set_blocked_limit(&mut self, limit: u32) {}
}
```

- `FieldBlock` and `FieldSection` own `Buf`/arena storage; nothing borrows a reactor
  buffer. `decode` takes ownership so a QPACK block that references missing dynamic
  entries can be retained and retried after encoder instructions arrive.
- QPACK blocked sections are bounded by the advertised `SETTINGS_QPACK_BLOCKED_STREAMS`
  and header-list limits; `resume` returns the sections that became decodable, with
  their arenas. HPACK never blocks and treats `resume` as a no-op.
- `Hpack` implements the same trait for HTTP/2: static table, dynamic table, Huffman,
  size updates, entry overhead, EOS/padding rules, decompression-bomb limits.
- `Qpack` implements it for HTTP/3: static table, dynamic table, encoder/decoder
  instructions, blocked-stream limits, Huffman. The h3 engine feeds control-stream bytes
  to `control` and drains `take_control_output` into QUIC unidirectional streams.
- One call per field block, not per field. The field model and arena are shared; the
  wire format and table state are not. An engine may hold its codec as
  `Box<dyn HeaderCodec>` or as a concrete type; either way the dynamic dispatch is per
  block, not per header.
- A discarded block is still decoded to keep the table in sync, or the connection
  closes.

### 7.6 HTTP/1.1 engine

Sans-io state machine: `ReadHead → Body → Write → KeepAlive | Close | Tunnel`. It
consumes `Buf` chunks and emits `Buf` chunks.

```rust
pub struct Engine { /* parser state, limits, not I/O */ }
impl Engine {
    pub fn feed(&mut self, buf: Buf) -> Result<()>;
    pub fn next_event(&mut self) -> Option<Event>;
    pub fn respond(&mut self, status, headers, body) -> Result<()>;
    pub fn write_body(&mut self, src: &[u8]) -> Result<()>;
    pub fn finish(&mut self) -> Result<()>;
    pub fn take_output(&mut self) -> Option<Buf>;
    pub fn detach(self) -> (Vec<Buf>, TunnelState); // CONNECT / 101
}
```

- Framing resolved at end-of-headers from method/status/version/fields; HEAD, 1xx, 204,
  304, and 2xx CONNECT have no body; `Chunked` must be final; conflicting CL/TE is
  fatal; duplicate CL only as an equal comma-list; chunk sizes overflow-checked;
  obs-fold, whitespace before a colon, and bare CR rejected; bare LF rejected
  deliberately.
- Headers: `Host` required exactly once on HTTP/1.1 requests; absolute-form wins; same-
  name order preserved.
- Pipelining: buffered `Buf`s beyond the current request stay queued; one request is
  parsed at a time and responses are written in order; the next head is not parsed until
  the previous exchange is complete.
- `Expect: 100-continue`: the engine never waits. The first body read or an explicit
  `continue_()` emits `100`; a final response before that skips it.
- CONNECT/`101`: the engine detaches into an opaque tunnel, returning buffered bytes and
  switching modes; no HTTP parsing remains.
- HTTP/1.0: close-delimited responses, no chunked, no 1xx, keep-alive only by token.
- Body bytes are `Buf` slices: zero-copy to proxy forwarding, worker queues, or response
  writes. A worker never sees a socket.

### 7.7 HTTP/2 engine

One connection, many streams, per RFC 9113. It consumes `Buf` chunks and emits `Buf`
chunks; frames may span buffer boundaries (a 9-byte frame header is assembled, DATA
payloads are split into zero-copy `Buf` slices).

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
- CONNECT: `:method=CONNECT` with `:authority` only and no `:scheme`/`:path`; a 2xx
  response switches the stream to a DATA tunnel with no HTTP parsing. This is plain
  CONNECT, not RFC 8441 extended CONNECT, and it is what the proxy uses for h1↔h2
  tunnels.
- Flow control: DATA only, padding counted. The stream window is replenished when the
  consumer takes the bytes (chunk dropped or `consume(n)`); the connection window is
  replenished as DATA is buffered into a bounded per-stream queue, up to a global
  buffered-bytes cap, so one slow stream cannot stall the connection. Zero/overflow
  increments are errors; a settings change moves every stream window by the delta and
  may make it negative.
- SETTINGS: own sent first, peer applied in order, unknown ignored, ACK immediately;
  server never sends ENABLE_PUSH; client sends 0; HEADER_TABLE_SIZE takes effect after
  ACK and forces a dynamic-table size update.
- HPACK via `HeaderCodec`; a discarded block is still decoded for table sync.
- Writes: control frames first, then DATA round-robin across positive-window streams;
  field blocks are atomic queue items; frames coalesce into one `Buf`/`writev`.
- Errors: one GOAWAY with the highest peer stream acted on on connection error; RST on
  stream error; no RST in response to RST; if one frame causes both, connection error
  wins.
- Graceful shutdown: `GOAWAY(2^31-1, NO_ERROR)`, drain, then the final GOAWAY with the
  real last stream.
- Early response: allowed; remaining DATA is discarded and the stream reset NO_ERROR
  after the response when it cannot be drained.

### 7.8 HTTP/3 engine

HTTP/3 runs over QUIC, not over `Buf`-stream TCP. `quinn-proto` supplies QUIC; the
reactor supplies UDP datagram completions and timers; `h3::Engine` supplies HTTP/3.

- QUIC endpoint: one UDP socket, quinn `Endpoint`, many connections. Datagram
  completions feed `Endpoint`/`Connection`; `poll_transmit` output becomes `SendTo`
  completions; `poll_timeout` becomes a `Timeout` op. No networking code in quinn is
  used; the reactor owns the socket.
- h3 control plane: control stream with SETTINGS and GOAWAY; QPACK encoder and decoder
  unidirectional streams; MAX_PUSH_ID/CANCEL_PUSH rejected/ignored (push is not
  implemented).
- Requests: one client-initiated bidirectional stream per exchange. HEADERS (QPACK),
  DATA, trailers as a final HEADERS; `content-length` check; pseudo-headers and
  forbidden fields validated as in HTTP/2.
- Flow control comes from QUIC stream and connection windows; the h3 engine exposes
  credit and backpressure through the same worker model. QPACK blocked-stream limits
  apply.
- ALPN `h3`; TLS 1.3 is inherent to QUIC; certificate configuration is shared with the
  TCP TLS path through rustls.
- No push; 0-RTT is a later optimization; connection migration is quinn's business.
- Listener equivalent: `QuicEndpoint` on UDP; no TCP accept. `Server`/`Client` gain
  `h3` configuration. HTTP/3 is selected explicitly (scheme, configuration, ALPN);
  Alt-Svc discovery and Happy Eyeballs racing are out of v1.

The engine is concrete, not a generic "HTTP over streams" abstraction. It shares typed
heads, `Headers`, trailers, `BodyLength` semantics, QPACK codec infrastructure, and the
worker model with h2 — which is exactly the payoff of those abstractions.

### 7.9 Worker model

Handlers are synchronous functions on a bounded pool. They never see a socket; they
read body chunks from a bounded queue and write responses into another. Blocking on
those queues (or on a database) is fine — that is what workers are for — and is distinct
from blocking socket I/O.

```rust
pub trait Handler: Send + Sync + 'static {
    fn handle(&self, request: &mut Request<'_>, response: &mut Response<'_>);
}

impl Request<'_> {
    pub fn head(&self) -> &RequestHead;
    pub fn body(&mut self) -> &mut BodyStream<'_>;
}

impl BodyStream<'_> {
    pub fn read(&mut self, dst: &mut [u8]) -> Result<usize>;
    pub fn next_chunk(&mut self) -> Option<DataChunk>;   // zero-copy for h1/h2/h3
    pub fn trailers(&self) -> Option<&Headers>;          // after end-of-body
}

impl Response<'_> {
    pub fn status(&mut self, status: StatusCode);
    pub fn headers(&mut self, headers: &[Header<'_>]);
    pub fn content_length(&mut self, len: u64);          // optional hint
    pub fn write(&mut self, buf: &[u8]) -> Result<()>;
    pub fn finish(self) -> Result<()>;
    pub fn finish_with_trailers(self, trailers: &[Header<'_>]) -> Result<()>;
}
```

The handler never chooses a transfer coding. `content_length(n)` is an optional hint:
when set, HTTP/1.1 sends that length; when not, HTTP/1.1 streams chunked and HTTP/1.0
closes; HTTP/2/3 always use DATA. HEAD, 204, 304, and 1xx suppress the body. Trailers
require chunked/unknown-length framing; declaring a length and then trailers is an
error. Chunk framing is serialized directly into a pooled `Buf`, so chunked responses
do not allocate per chunk.

Returning from `handle` without finishing auto-finishes on `Response` drop: nothing
written means `200` with `Content-Length: 0`; headers or body already written are sent
as written. A panic maps to a closed connection (HTTP/1.1) or `INTERNAL_ERROR` reset
(HTTP/2/3). Writing after `finish` is a typed error. Exceeding the server body cap
before headers produces `413`; after headers it closes (HTTP/1.1) or resets the stream.

- HTTP/1.1: one job per exchange. The reactor delivers zero-copy `Buf` body chunks;
  when the job queue is full it stops submitting reads for that socket (TCP
  backpressure). The first body read on a `100-continue` exchange queues the interim
  response. Response commands are serialized into `Buf`s by the reactor.
- HTTP/2: one job per stream. `DataChunk` is a `Buf` plus stream id and credit; drop
  batches credit back to the reactor.
- HTTP/3: one job per request stream, same shape over QUIC stream chunks.
- The pool is shared and bounded. When the dispatch queue is full, HTTP/2/3 streams are
  refused with `REFUSED_STREAM`; HTTP/1.1 stops dispatching under a request deadline;
  optional `503` shedding.
- Cancellation is an `Arc<StreamState>` atomic plus queue condvars; reset/close poisons
  both queues and wakes a worker parked on a full response queue or empty body queue.
  Credit is accumulated in an atomic and the reactor is woken on a threshold or stream
  end, not per chunk.
- Worker waits are bounded by the exchange deadlines. `BodyStream::read` waits on the
  queue with the body deadline and returns a typed timeout when it expires; the reactor
  closes the connection (HTTP/1.1) or resets the stream (HTTP/2/3) at the same deadline.
  A trickling client can therefore pin a worker for at most `body_timeout`; per-IP and
  connection limits bound how many workers that can consume. A response write has the
  same deadline through `write_timeout`. This is the cost of synchronous handlers and it
  is documented rather than hidden.
- A response that cannot flush before `write_timeout` is reset (`CANCEL`) so a slow peer
  cannot pin workers. Dropping a `Responder` resets. If an HTTP/1.1 handler responds
  before reading the body, the reactor drains and discards up to the body limit or
  closes; the connection is reused only when drained.
- A panicking handler is caught: HTTP/1.1 closes the connection, HTTP/2/3 resets the
  stream. Handler execution is not timed out by the library; a stuck handler pins its
  worker, which is why the pool is bounded.

### 7.10 Server

Reactors + worker pool. No accept thread, no connection thread.

- Listener session submits `Accept`; completed sockets are registered with a server
  session. Backend differences (multishot accept on io_uring, `AcceptEx` on Windows,
  readiness accept on epoll/kqueue) are inside the driver.
- Protocol negotiation: a plaintext listener is configured `H1`, `H2PriorKnown`, or
  `Both`. `Both` buffers the first bytes until they match the 24-byte HTTP/2 preface or
  cannot, then feeds those `Buf`s to the chosen engine — no copy, no re-read, no
  protocol renegotiation. TLS uses ALPN (`http/1.1`/`h2`); QUIC uses ALPN `h3` or an
  explicit `h3` endpoint configuration.
- HTTP/1.1: an idle keep-alive connection keeps one small read completion armed; a
  complete head creates one job; body/response `Buf`s flow through queues; the socket
  stays in the driver.
- HTTP/2: per-stream jobs.
- HTTP/3: QUIC connections live in the endpoint session; per-request jobs.
- Limits: connections, per-IP, concurrent streams, body size, header size, request
  deadlines, idle and write deadlines. `body (server)` is optional (`None` = unlimited),
  enforced by the server, not the framing layer.
- Graceful shutdown: stop accepting, stop dispatching, close idle HTTP/1.1, GOAWAY and
  drain HTTP/2, GOAWAY and drain HTTP/3, deadline, reset. A cloneable `Shutdown` handle
  triggers it; `Server::start` returns `ServerHandle` with `shutdown`/`join`; `run` is
  `start` + join.
- High-level server does not expose CONNECT/`101` tunnels in v1; the low-level API
  does, and the proxy uses it.

### 7.11 Client

`Client` owns one reactor thread (configurable) and a pool.

- `send` submits a request and waits on a bounded response channel. The caller blocks —
  it is a synchronous API — but the socket is driven by the reactor and never blocked
  on by the caller.
- `client::Request` carries an absolute target (scheme, authority, path); HTTP/2/3
  pseudo-headers derive from it.
- Name resolution never runs on the reactor: the caller or an optional resolver pool
  turns the authority into `SocketAddr`s; `Connect` is a completion.
- HTTP/1.1: pool of idle connections by origin; one in-flight request per connection;
  the per-origin lock only takes/returns an idle connection. A `Response` owns its head
  and body stream; the connection returns to the pool when the body ends. A response
  dropped mid-body closes the connection.
- HTTP/2/3: shared multiplexed connections; per-stream queues bounded; the reactor
  withholds credit instead of blocking when a caller is slow; dropping a response sends
  `RST_STREAM`/`STOP_SENDING`.
- Request bodies: buffered (`Buf`) via `send`, or streamed via `send_streaming(head)`
  returning `RequestBodyWriter` and `PendingResponse`; streamed requests are never
  retried. Responses always stream.
- No redirects, retries, cookies, or compression; one opt-in stale-connection retry.
- Connection failures, timeouts, and resets surface as typed errors.

### 7.12 Proxy

`examples/proxy` is a working reverse proxy built only on public low-level types, with
no worker pool:

- One reactor session can own both client and upstream sockets; it submits completions
  on both and pumps `Buf`s between the h1/h2 engines. No fd-per-session restriction, no
  blocking.
- HTTP/1.1↔HTTP/1.1, HTTP/1.1↔HTTP/2, HTTP/2↔HTTP/2. HTTP/2 streams map 1:1; DATA is
  forwarded as zero-copy `Buf` slices; client credit is released only after the
  upstream write completes; trailers, RST_STREAM, and GOAWAY propagate.
- CONNECT/`101`: both engines detach into tunnel state and the session pumps opaque
  bytes. HTTP/2 uses plain CONNECT (`:authority` only); extended CONNECT is out of v1.
- Strip `Connection`-named fields, regenerate `Host` from an absolute target, forward
  1xx responses, never reorder fields, never reuse an upstream connection to an
  HTTP/1.0 client.
- Its integration tests are the completeness test for the low-level API. If the proxy
  needs a private API, the design failed. HTTP/3 proxy support follows h3 in M8.

## 8. Public surface

```
libhttp
  reactor     { Reactor, ReactorHandle, Session, Step, Io, Op, Event,
                SocketId, ListenerId, OpId, ReactorConfig, Backend }
  buffer      { Buf, BufMut, WriteBufs, Pool, PoolHandle }
  message     { Method, StatusCode, Version, HeaderName, HeaderValue, Header, Headers,
                RequestHead, RequestTarget, Body, BodyLength }
  codec       { HeaderCodec, Decode, FieldSection, FieldBlock }   // h2/h3 shared
  h1          { Engine, Event, ResponseWriter, Config }
  h2          { Engine, Event, Events, StreamId, DataChunk, Responder,
                ResponseWriter, DataStream, ErrorCode, Config }
  h3          { Engine, QuicEndpoint, StreamId, ErrorCode, Config }
  server      { Server, ServerHandle, Handler, Request, Response, ServerConfig, Shutdown }
  client      { Client, ClientConfig, Request, Response, BodyStream,
                RequestBodyWriter, PendingResponse }
  worker      { WorkerPool, BoundedQueue }
  error       { Error, ProtocolError, ConnectionError, StreamError, Limit, Timeout }
```

Feature flags: `server`, `client`, `h1`, `h2`, `h3` (default), `tls` (default).
Backends are selected at runtime by `ReactorConfig`; Linux builds include io_uring and
epoll, macOS/BSD kqueue, Windows IOCP.

## 9. Performance model and measurement

Targets, measured and published:

- 10k idle connections of any protocol per reactor thread: slab slot + timer + socket +
  one 4 KiB read buffer (≈ 40 MiB of pooled data), no thread, no readiness rearm.
- Linux: io_uring path; one submission/completion per data operation, batched;
  no readiness rearm. Epoll fallback measured separately.
- Windows: IOCP overlapped operations; no readiness emulation on the primary path.
- macOS/BSD: kqueue with completion emulation; measured honestly.
- HTTP/1.1 low-level request: one `Read` completion, one `Write` completion, plus
  amortized submission; no intermediate copy of body bytes.
- HTTP/1.1 worker path: zero-copy body `Buf`s to the worker; response bytes copied once
  into a pooled `Buf`; header arena from a pool; no per-request heap allocation for
  heads, arenas, or queues after warm-up (the per-buffer refcount header is counted and
  measured separately).
- HTTP/2: zero-copy DATA `Buf` slices; stream credit released on consumption;
  connection credit against a bounded aggregate buffer; no unbounded write queue.
- HTTP/3: one copy from quinn's chunk to `Buf` at the adapter; zero-copy thereafter;
  QUIC flow control is the backpressure.
- Copies: kernel→`Buf`, `Buf`→kernel; TLS adds its boundary copies; HTTP/1.1 headers one
  bounded copy into the arena; h2/h3 headers decoded once.
- Linux performance gate, measured on a fixed host with the CPU model, kernel, rustc,
  and hyper version recorded: 1 KiB responses, keep-alive, concurrency 64 and 512, 30 s
  runs. Throughput at least 90% of hyper (tokio multi-thread, same cores), p50 at most
  1.10×, p99 at most 1.25×. Missing the gate is a release blocker that triggers profiling
  of the worker handoff and buffer path; it is not deferred to the end. Windows/macOS
  numbers are published separately, not extrapolated, and are report-only in v1.
- 10k idle connections: ≤ 100 MiB resident excluding kernel socket memory (≈ 40 MiB of
  pooled 4 KiB read buffers plus slab/timers), measured.

Harness: `criterion` for parsers, buffers, HPACK/QPACK, and codec microbenchmarks;
`oha`/`wrk2` for end-to-end; `strace -c`/ETW for syscalls; a counting allocator; `perf`;
a separate `tools/bench-compare` crate (path dependency on libhttp, hyper as its only
heavy dependency) runs both behind the same load generator. The comparative harness is
stood up in M1 and wired into the M3/M5 acceptance gates, not left for the end.

## 10. Correctness, safety, limits

- Malformed input is an error, never a panic. Errors map to the right HTTP/1.1 status
  (`400`, `408`, `413`, `431`, `505`) when a response is possible, otherwise close.
- Fuzzed: HTTP/1.1 request/response/chunked, HTTP/2 frames, HPACK, QPACK, h3 frames,
  buffer refcounting/splitting, and the public message types.
- A smuggling corpus: CL/TE conflicts, duplicate CL, whitespace variants, obs-fold,
  bare CR/LF, whitespace-split start lines, chunk-size oddities, undrained bodies.
- Conformance: unit/table-driven tests; `h2spec`/`h2load`; curl/nginx interop; QUIC/h3
  interop (quic-interop-runner or equivalent).
- Limits checked eagerly: header block, header count, request line, body, chunk size,
  trailer block, frame size, concurrent streams, HPACK/QPACK table, header list,
  read-ahead, global HTTP/2 buffered DATA. Defaults:

  | Limit | Default |
  |---|---|
  | request line | 8 KiB |
  | header block | 16 KiB |
  | header count | 100 |
  | body (server, optional) | 2 MiB |
  | chunk size | 16 KiB |
  | read buffer | 16 KiB class (8/16/32/64 available) |
  | write buffer | 32 KiB |
  | HTTP/2 frame | 16 KiB |
  | HTTP/2 concurrent streams | 100 |
  | HTTP/2 header list | 16 KiB |
  | HTTP/2 HPACK table | 4 KiB |
  | HTTP/2 stream window | 64 KiB |
  | HTTP/2 connection window | 1 MiB |
  | HTTP/2 global buffered DATA | 8 MiB |
  | HTTP/3 QPACK table | 4 KiB |
  | HTTP/3 concurrent streams | 100 |
  | timeouts (header/body/idle/write) | 10s / 30s / 60s / 30s |

- Unsafe is confined to the OS backends, the buffer read-into-spare path, and the QUIC
  adapter; all documented with safety comments. `unsafe_op_in_unsafe_fn` denied,
  `missing_docs` warned.
- io_uring is probed, never assumed; if it is unavailable or disabled, the epoll backend
  runs and reports that choice.
- No global mutable state; threads exist only as configured reactors, workers, and the
  client driver. No logging by default; counters exposed as a snapshot.

## 11. Crate layout and dependencies

Single crate, modules:

```
src/
  sys/        linux/{uring,epoll}, macos/kqueue, windows/iocp, common
  driver/     Op, Event, backend selection, timer heap, wake
  reactor/    Reactor, Session, Io, Step
  buffer/     Buf, BufMut, WriteBufs, Pool
  message/    typed heads, headers, targets
  codec/      hpack, qpack, HeaderCodec
  h1/ h2/ h3/ engines and events
  server/ client/ worker/
```

Dependencies:

- `libc` (Unix), `windows-sys` (Windows): OS surface.
- `io-uring`: Linux SQ/CQ; hand-rolling it would be worse code.
- `rustls` + platform verifier: TLS for HTTP/1.1 and HTTP/2; QUIC uses rustls through
  quinn-proto.
- `quinn-proto` (feature `h3`): QUIC transport; HTTP/3 framing and QPACK are ours.
- `criterion` (dev only).
- No `bytes` types in our public API or buffer core; `quinn-proto` uses it internally
  and the h3 adapter copies across that boundary. No `libringbuf`, `mio`, `http`,
  `httparse`, `h2`, `hyper`, or `tokio`.

Edition 2024, MSRV set by the CI toolchain. License MIT OR Apache-2.0. README:
philosophy, limits, guarantees, platform matrix, and measured numbers.

## 12. Milestones

**M0 — Scaffold and buffers.** Crate, cross-platform CI (Linux, macOS, Windows),
`Buf`/`BufMut`/`WriteBufs`/`Pool`, Miri and fuzz tests, benches, license.

**M1 — Completion driver and Linux reactor.** `Op`/`Event`/`Io`/`Session`, io_uring
backend, epoll fallback, timer heap, wake, accept/connect/read/write/datagrams, close
and cancel draining, 10k idle connection test, forced-backend tests, and the
comparative benchmark harness.

**M2 — HTTP/1.1 engine.** Sans-io parser/serializer over `Buf`, events, framing rules,
limits, chunked, trailers, 100-continue, upgrade/CONNECT detach, fuzz; in-memory tests.

**M3 — HTTP/1.1 server and client.** Worker pool, body/response queues, dispatch,
backpressure, timeouts, graceful shutdown, client pool and streaming. Acceptance:
curl/nginx interop, 10k idle, zero allocations per request after warm-up, slow-header
and slow-body tests, and the Linux performance gate.

**M4 — HTTP/2 engine.** Frames across buffers, stream states, HPACK, flow control,
settings, errors; in-memory tests.

**M5 — HTTP/2 server/client and proxy.** Per-stream jobs, responder channels, credit,
shutdown, h2c prior knowledge and H1/H2 `Both` sniffing; plain h2 CONNECT; h2spec/h2load;
proxy h1↔h1/h1↔h2/h2↔h2 with CONNECT; performance gate repeated under h2.

**M6 — macOS/BSD and Windows backends.** kqueue and IOCP drivers, socket differences,
cross-platform end-to-end tests, platform benchmarks.

**M7 — TLS.** rustls for HTTP/1.1 and HTTP/2, ALPN, certificate config, `close_notify`,
TLS benchmarks.

**M8 — HTTP/3.** quinn-proto integration, QPACK, h3 framing, endpoint, server/client,
interop, benchmarks; h3 proxy.

**H0 — QUIC/h3 spike (parallel from M1).** Drive quinn-proto from our datagram
completions on loopback, prototype QPACK static-table decode and h3 frames, verify the
`Buf` boundary and timer integration.

## 13. What is left to the builder

- `Buf` pool internals, size classes, refcount/allocator strategy, and free-list
  contention tuning.
- io_uring submission batching, multishot accept, and registered files; the one-read/
  one-write-per-socket contract stays fixed in v1 (no multishot recv or provided-buffer
  rings). Epoll emulation details; kqueue/IOCP specifics; close/cancel/drain mechanics.
- `WriteBufs` chain limits and the partial-write resubmission policy.
- Queue/channel implementation (preallocated), wake batching, credit thresholds.
- Worker pool sizing and dispatch policies.
- Header intern set and index layout; HPACK/QPACK encoder strategies.
- QUIC adapter scope, quinn-proto version, h3 configuration; measure the h3 boundary
  copy, and do not fork quinn-proto in v1.
- TLS adapter details (cert loading, ALPN failure policy).
- Test corpus curation, h2spec/h2load/quic-interop invocations, platform CI matrix.

## 14. Calls made in this document (check these first)

1. HTTP/1.1, HTTP/2, and HTTP/3 in v1; h3 is not deferred.
2. Completion-based I/O; io_uring primary on Linux with epoll fallback; native kqueue
   and IOCP; no mio.
3. No blocking socket I/O, no thread-per-connection, no async.
4. The driver owns sockets; a session can own several sockets; engines are sans-io.
5. `Buf`/`BufMut` are our pooled refcounted buffers; no `bytes` types in our public API
   or buffer core, no `libringbuf`. Ring buffers are the slower shape for completion
   I/O.
6. QUIC transport is `quinn-proto`; h3 framing/QPACK ours; its internal `bytes` stays
   behind the adapter.
7. `HeaderCodec` is the one new abstraction that exists for h3 and pays immediately.
8. Worker pool for all protocols; zero-copy `Buf` body handoff; response bytes copied
   once into a pooled `Buf`.
9. Cancellation via shared state; batched credit; no per-chunk eventfd.
10. `Session` is the only reactor-facing trait object; one call per completion.
11. Single crate, feature-gated modules; dependencies are `libc`, `windows-sys`,
    `io-uring`, `rustls`, `quinn-proto`, `criterion`.
12. TLS before h3 (h3 requires TLS 1.3); both use rustls.
13. HTTP/2 push and priority omitted; plain h2 CONNECT is in; extended CONNECT is out;
    padding accepted.
14. The server body cap is optional policy (`None` = unlimited).
15. All major platforms are first-class: Linux, macOS/BSD, Windows, with cross-platform
    CI from M0.
16. Writes take a `WriteBufs` chain and use `WRITEV`/`WSASend`/`writev`; frame headers
    and payloads are one vectored write, so forwarded DATA stays zero-copy.
17. Close is deferred: pending buffers return through their completions, `Closed`
    marks completion, and `OpId`s are reusable only after completion.
18. `Session::sockets()` declares ownership at registration; a connected socket is
    mapped to the submitting session when it completes.
19. Worker body reads carry the body deadline and return a typed timeout; a slowloris
    can pin a worker for at most that deadline, bounded further by per-IP limits.
20. One read and one write per stream socket is a fixed contract in v1; no multishot
    recv or provided-buffer rings.
21. QPACK blocked sections are owned and retained by the codec, bounded by the blocked-
    stream setting; `Decode::Blocked` plus `resume` is the API.
22. A plaintext listener can serve `H1`, `H2PriorKnown`, or `Both`; `Both` sniffs the
    24-byte preface from buffered `Buf`s. HTTP/2 CONNECT is plain CONNECT, not RFC 8441.
23. The Linux performance gate is explicit: ≥90% of hyper throughput, p50 ≤1.10×,
    p99 ≤1.25× for 1 KiB responses at concurrency 64/512 on a fixed host.
24. Headers are concrete types (`HeaderName`, `HeaderValue`, `Header`, `Headers`); no
    per-field trait object. Traits remain at the codec/session/handler boundaries.
25. No fork of `quinn-proto` in v1; the h3 boundary copy is measured. `bytes` trait
    definitions may be used if interop demands, but its buffer implementations stay out
    of our core.
