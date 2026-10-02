# First WebSocket trading gateway

## Intent and scope

Implement the first gateway slice agreed in the architecture discussion: web/mobile-compatible order submission through a Java WebSocket service, reusing EngineGateway and the existing Aeron/SBE/Archive processing path. This is a local learning service, not a production identity or account system. Retire the existing gRPC interface and protobuf generation as requested during PR preparation; retain historical benchmark data with its revision clearly marked. No new transport benchmark or production deployment is part of this slice.

## Contract

- Listen on IPv4 loopback, default port 8080, WebSocket path `/orders`.
- One JSON text message contains `requestId` (full UUID) and exactly one of `place` or `cancel`.
- Place has `orderId`, `side` (`BID`/`ASK`), `priceTicks`, `quantityLots`; cancel has `orderId`.
- All order identifiers, tick prices, lot quantities, and corresponding response fields are decimal strings, preserving Java long values in browsers. Request IDs and commands retain the engine's retry semantics.
- Reject unknown/duplicate fields, malformed JSON, non-string long fields, invalid UUIDs, nonpositive prices/quantities, and ambiguous commands before admission.
- Results contain requestId and exactly one place/cancel/reject result. Errors contain requestId (null if decoding failed), code, message, and outcomeUnknown.
- Distinguish queue/connection admission overload (RESOURCE_EXHAUSTED, not admitted) from unavailable execution results (UNAVAILABLE, conservatively outcome unknown). Business rejection remains a normal result.

## Components and concurrency

- WebSocketCommandCodec: strict JSON/domain conversion with Gson 2.14.0; no dependency on gRPC DTOs.
- WebSocketGateway: Netty 4.2.18.Final HTTP upgrade, bounded frame aggregation, per-connection asynchronous response routing. Each connection has its own handler; writes and state updates occur on its event loop. EngineGateway callbacks only schedule completion work, never perform socket I/O or JSON conversion on the Aeron worker.
- WebSocketGatewayServer: CLI/resource ownership around the existing shared local driver, IPC request stream 1 and reply stream 2. Configurable port and gateway queue/window limits.
- GatewayOverloadedException: identifies failed queue admission without parsing exception text.

## Limits and lifecycle

16 KiB maximum inbound message/HTTP aggregate; 32 outstanding responses per connection; 128 simultaneous connections; 64/128 KiB outbound watermarks; close slow/unwritable connections. Stop idle connections after 60 seconds without inbound data. Bound response size to 1 MiB. Netty handles ping/pong and close handshakes. Never cancel an already admitted command merely because its socket closes. Stop the WebSocket listener/connections, drain EngineGateway, then close Aeron resources.

## Production boundary

Loopback-only plain ws is intentional for this lesson. Browser Origin headers are restricted to HTTP(S) localhost/127.0.0.1 origins; native clients may omit Origin. This local guard is not authentication. WSS/TLS termination, authenticated identities, production origin policy, authorization/ownership, risk/account state, multi-gateway reply isolation, and replicated engine availability are subsequent work. The current shared reply stream and UUID namespace are suitable for the trusted lab, not isolated tenants. No claim of lower latency than gRPC is made.

## Verification

Test strict decoding and exact 64-bit values; results/trades/rejections; independent connections and out-of-order completions; bounded admission and slow writers; malformed/fragmented/binary/oversized frames; disconnect without undo and same-ID retry; listener shutdown and failed bind cleanup. Run one real WebSocket → gateway → Aeron → recorded engine integration with temporary Archive storage, then the complete Maven suite and Spotless.
