# libhttp dispatch — Design

**Status:** subsystem design. Owns everything between "a request head exists" and "a handler returns a response": queues, caps, deadlines, cancellation and the wake path.
**Parent:** `2026-10-26-libhttp-design.md`.
**Consumes:** `conn` (`Exchange` payloads, `Body`), transport (`WakeHandle`, `TimerKind::Worker`, `CreditSink`).

---

## 1. Scope

**Owns:** the dispatcher, the exchange, the two queues, the two caps, the deadline protocol, cancellation semantics, and the worker-facing API.

**Does not own:** thread creation (`threads`), protocol parsing (`h1`/`h2`/`h3`), sockets (transport). It is a dispatcher, not an executor: it has no thread and no loop. With `threads` disabled, this module still exists — an application that writes its own loop uses it directly.

---

## 2. Types

```rust
pub struct Dispatcher { /* ... */ }
impl Dispatcher {
    pub fn new(config: DispatchConfig, wake: WakeHandle) -> Dispatcher;
    pub fn submit(&mut self, exchange: Exchange) -> Result<(), Full>;
    pub fn drain_completions(&mut self) -> impl Iterator<Item = Completion> + '_;
    pub fn cancel(&mut self, exchange: ExchangeId, reason: CancelReason);
    pub fn parked(&self) -> Parked;              // { total, max_per_connection }
    pub fn on_worker_deadline(&mut self, exchange: ExchangeId);
}

// worker side; no &mut Driver, no socket, no connection
impl Dispatcher {
    pub fn pop(&self) -> Option<Exchange>;
    pub fn finish(&self, exchange: Exchange, response: Response);
}

pub struct Exchange { /* head, bounded body mailbox, response writer, cancel flag, conn + stream */ }
impl Exchange {
    pub fn head(&self) -> HeadView<'_>;
    pub fn connection(&self) -> (ConnId, StreamId);   // for the pump, not for I/O
    pub fn read_body(&mut self, out: &mut [u8]) -> Result<usize, ExchangeError>;
    pub fn write_body(&mut self, body: Body) -> Result<Flow, ExchangeError>;
    pub fn write_head(&mut self, head: Head) -> Result<(), ExchangeError>;
    pub fn end(&mut self) -> Result<(), ExchangeError>;
    pub fn trailers(&mut self, fields: Head) -> Result<(), ExchangeError>;
}

#[non_exhaustive]
pub enum ExchangeError { Reset, Closed, MailboxFull, BodyTooLarge }

pub struct DispatchConfig {
    pub queue: usize,            // pump→worker, default 1024
    pub completions: usize,      // worker→pump, default 1024
    pub body_mailbox: usize,     // bytes per exchange, default 64 KiB
    pub per_connection: usize,   // parked workers, default 8
    pub global: usize,           // parked workers, default 64
}
impl Default for DispatchConfig;
```

`ExchangeId` is a `u64` unique per process lifetime, not per connection: it is what `TimerKind::Worker { exchange }` carries, and what `cancel` names.

---

## 3. Contracts

### 3.1 Topology

One shared pump→worker queue. One `WakeHandle` per shard (§2 of transport). A worker never holds a `&mut Driver` and never touches a socket; when it has a completion or a body request it calls the handle for the exchange's shard, which marks that connection active on that pump.

The queue is shared rather than per-shard because a blocked handler must not be able to starve a shard's dispatch: the pump that owns the connection is the only thing that admits work for it, and a single queue means an idle worker on the wrong shard can still take it. The topology is stated rather than accidental — the alternative, per-shard queues, gives locality and isolates a stuck shard, and is not what v1 ships.

### 3.2 Admission and the two caps

`submit` refuses when the pump→worker queue is full (`Full`), and the pump responds with a **not-processed** code: `REFUSED_STREAM` on h2, `H3_REQUEST_REJECTED` on h3, a 503 on h1. That is correct precisely because nothing has run yet.

Two parked caps, both enforced at admission:

- `per_connection` (8): a peer that opens many streams and reads none cannot convert one connection into a parking lot.
- `global` (64): the per-connection cap bounds nothing on its own — 100 connections × 8 exhausts a 64-thread pool.

Exceeding either is also a refusal, with the same not-processed codes. A refusal is never a reset of a stream that may already have run: the distinction between "we refused it" and "it may have run" is the whole reason the codes differ.

### 3.3 The exchange

An `Exchange` is the only thing a worker sees. It carries:

- the request head, owned (`Head`, `conn` §2), so it outlives the read chunk;
- a bounded body mailbox, filled by the pump from `Body` handles, so a worker reading a body is reading memory, never a socket;
- a response writer that accepts a head, body handles and trailers;
- a per-stream cancel flag.

`read_body` returns `Err(ExchangeError::Reset)` after a reset, and `Ok(0)` only at a clean end. A worker that has been cancelled and returns late cannot write into returned buffers: every method checks the flag and fails.

### 3.4 Time

The dispatcher owns no clock. A deadline is a driver-issued `Deadline` the pump sets with `TimerKind::Worker { exchange }`; the pump fires it and calls `on_worker_deadline`, which sets the cancel flag **and** wakes the worker. Only `wait` evaluates time; a worker that parks has no timer of its own, so the "two timer owners" failure cannot occur.

A worker blocked inside its own database cannot be cancelled by anyone. It is cancelled on return, and its write fails. That is stated rather than papered over, because there is no way for this library to interrupt user code.

### 3.5 Cancellation

`cancel` marks the exchange, drops its mailbox, and returns its credit to the source connection. The worker's next call fails with `Reset`. If the worker never returns, the exchange's memory is still reclaimed — the mailbox is the dispatcher's, not the worker's.

A cancelled exchange's stream is reset with the *may-have-run* code: `CANCEL` on h2, `H3_REQUEST_CANCELLED` on h3, a 504 on h1 if the head was already sent. It is never `REFUSED_STREAM`, because that promises the peer nothing ran.

### 3.6 The pump runs no application code

`submit`, `drain_completions`, `cancel` and `on_worker_deadline` may be called on the pump thread; none of them calls a handler. This invariant is what makes the pump's timing claims true, and `Server` asserts it in debug builds.

### 3.7 Completion

`finish` moves the response to the completion queue and wakes the pump. The pump drains completions before draining output, because a completion can be the thing that produces output. A completion for a closed connection is dropped, and the exchange's buffers are reclaimed with it.

---

## 4. What a builder decides

Default queue and mailbox depths beyond the stated numbers, the slab layout for exchanges, and whether the wake bitmap is a per-connection `AtomicBool` or a mutex-protected vector.

Not theirs: the two caps and their refusal codes, the single-clock rule, cancellation semantics and its codes, the no-application-code invariant, and the exchange's "fail after reset" contract.
