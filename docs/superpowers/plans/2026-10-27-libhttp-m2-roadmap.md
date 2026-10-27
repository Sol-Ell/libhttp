# libhttp M2 Roadmap — Runtime + HTTP/1.1

**Status:** planning. M1 transport spine is complete and green on all CI platforms.

M2 is too broad for one implementation plan: it combines a new public Runtime,
worker/admission layer, HTTP/1.1 server, streaming bodies, deferred completion,
HTTP/1.1 client, connection pooling, DNS, and the G1 gate. This roadmap splits it
into two independently shippable plans, each with its own tests and exit gate.

## M2a — Runtime and HTTP/1.1 server

**Exit:** a real HTTP/1.1 server on the M1 reactors; keep-alive and pipelining;
streaming request/response bodies; typed headers; deferred completion with the
approved fallback; h1 smuggling corpus; server half of G1 measured.

Planned tasks:

1. Public HTTP primitives and errors: `Method`, `StatusCode`, `Version`,
   `HeaderName`, `HeaderValue`, `HeaderRef`, `HeaderMap`, `Header`, `HeaderError`,
   `Error`, `TimeoutKind`, `Target`, `Uri`.
2. Public `Runtime`/`RuntimeBuilder`/`RuntimeHandle`: reactor threads, lazy worker
   and resolver pools, root/component shutdown, stats, private-runtime builders.
3. Worker pool, dispatch queue, admission, deadlines, cancellation; no reactor
   ever runs handler code.
4. HTTP/1.1 head parser/serializer: request line, headers, framing, Host/absolute
   form, strict CRLF, limits, smuggling defenses.
5. `BodyReader`/`BodyWriter`/`BodyChunk`: streaming, limits, trailers, blocking
   app-thread semantics, body-cancel propagation.
6. `Server`/`ServerBuilder`/`ServerHandle`: listeners, accept, connection table,
   keep-alive, pipelining, graceful shutdown, h1 ordering.
7. `Handler`/`Request`/`Response`: typed/raw headers, streaming, errors, panics,
   h1 response framing.
8. Deferred completion: `Request::defer`, `defer_with_fallback`, owned fields but
   borrowed `&DeferredFailure`, `&mut Response`, default 504/503/500, h1
   close/drain, worker fallback job.
9. Server integration: smuggling corpus, fuzz seeds, keep-alive/pipelining,
   deferred-streaming/trailers/order tests, combined runtime seed, G1 server
   measurements.

## M2b — HTTP/1.1 client and combined G1

**Exit:** blocking `send`/`send_with`, callback `fetch`/`fetch_with`, pool/DNS/
connect, client interop with external servers, client half of G1, combined
server+client on one Runtime.

Planned tasks:

1. `Client`/`ClientBuilder`/`ClientHandle` and connection pool keyed by
   `(scheme, host, port, ALPN)`; no TLS yet.
2. DNS resolver threads, Happy Eyeballs, connect deadlines/cancellation.
3. Client request serialization and upload bodies; `send`/`send_with`.
4. Client response parsing/streaming and `ClientResponse`; connection reuse rules.
5. `fetch`/`fetch_with` callback queue, `max_client_callbacks`, exactly-once
   delivery, cancellation.
6. Client limits/timeouts/retries; interop with an external HTTP/1.1 server.
7. Combined Runtime server+client test; G1 client/server numbers; CI matrix.

## Dependencies

- M2a depends only on M1 (`src/rt`) and the approved design docs.
- M2b depends on M2a's public primitives, Runtime, body types and server for
  combined/interop tests.
- M2a gates on a **server-only G1** with an external load generator; M2b gates on
  the final combined client+server G1. A G1 miss blocks the phase, never relaxes
  correctness.

## Next step

Write the detailed M2a implementation plan first; M2b follows once M2a's public
surface is stable. No TLS, HTTP/2 or HTTP/3 in M2.
