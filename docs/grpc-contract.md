# gRPC contract

The gateway's external interface is defined in
[`exchange.proto`](../src/main/proto/exchange/v1/exchange.proto). Java bindings are
generated from this file; the future Go backend can generate its client from the same contract.
The Java contract, mapper, gRPC service, gateway server, and demo client are implemented.
The Go client is a future consumer of the contract.

## Generate the Java code

From the repository root, using Java 25:

```sh
mvn generate-sources
mvn verify
```

Generation also runs automatically before compilation. The Maven plugin selects
the native compiler for the current OS and CPU, including Apple Silicon. Its
execution is enabled during Maven project import for Zed's Java language server.

Generated files are under `target/`, so they are not committed or edited by hand:

| Output directory | Contents |
| --- | --- |
| `target/generated-sources/protobuf/java` | Message and enum classes in `dev.sam.exchange.gateway.proto` |
| `target/generated-sources/protobuf/grpc-java` | `ExchangeServiceGrpc`, containing the service base class and client stubs |
| `target/generated-sources/sbe` | Existing SBE codecs for the Archive request log |

Protobuf generation follows the [gRPC Java Maven example](https://github.com/grpc/grpc-java/blob/v1.84.0/examples/pom.xml).
The gRPC libraries and generator share one version; the protobuf runtime and compiler
also share one version.

## Read the schema

```proto
service ExchangeService {
  rpc Submit(SubmitRequest) returns (SubmitResponse);
}
```

This is a unary RPC: one submitted command receives one response. The full method
name is `exchange.v1.ExchangeService/Submit`.

- `message` defines the data sent across the connection, much like the shape of a Java record.
- `oneof command` holds a place order or cancel order. It allows **at most one**;
  it does not require either to be set.
- `oneof result` holds a place, cancel, or rejection result.
- `repeated Trade trades` is an ordered list of fills.
- The numbers after `=` are wire field identifiers, not default values. Preserve
  them when evolving the API; reserve removed field numbers and names rather than
  reusing them.

The protobuf classes have a separate package from the engine records.
`GrpcCommandMapper` translates between them, keeping the matching engine independent
of gRPC and generated protobuf classes.

## Values and request identity

`request_id` carries the existing UUID as text, using the full 8-4-4-4-12 format.
Uppercase hexadecimal is accepted; shortened or oversized groups are rejected.
A logical request keeps the same
UUID and command across Go retries, gRPC calls, and Aeron retries. A gRPC timeout
does not prove that the engine rejected or failed to execute the command.

All IDs, prices, quantities, and remaining quantities use signed `int64`, matching
Java `long` and Go `int64`. Prices remain integer ticks and quantities remain lots.
Order IDs retain the engine's current signed-long range, including zero and negative
values; the adapter must not silently introduce a different ID range.

Proto3 enum defaults use an explicit `UNSPECIFIED` value. The adapter must validate
the UUID, require a command, reject unspecified or unknown sides, and enforce the
engine's positive price/quantity rules. The schema itself does not enforce those rules.

An ordinary engine rejection, such as `DUPLICATE_ORDER_ID` or `REQUEST_ID_CONFLICT`,
is a `SubmitResponse` containing `RejectResult`. Cancelling a missing order is a
normal `CancelResult` with `cancelled = false`. Invalid input and transport failures
are reported through gRPC status by the adapter.

## Run the gateway

Use the class play buttons in Zed, starting `AeronEngineServer`, then
`GrpcGatewayServer`, then `GrpcGatewayClient`. The engine and gateway must share
`java.io.tmpdir` and the existing Aeron JVM option:

```text
--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
```

The gateway connects to the engine's Media Driver, publishes requests on IPC stream 1,
and receives replies on stream 2. It starts an `EngineGateway` with capacity 128
before opening its plaintext gRPC listener on port 50051. The client connects to
`localhost:50051` and sets a five-second RPC deadline.

The client submits a ten-lot bid at 100 with order ID 1001 and a new UUID. Its result
depends on the recovered book: it can rest, trade, or be rejected if that order ID
already rests on the book. A new client run is a new logical request, not a retry
of the previous run. For a retry, retain the original `SubmitRequest`.

Zed launches Maven with `exec.args` for the target Java class. The SBE execution uses
an explicit `commandlineArgs` schema path so these launch arguments do not leak into
code generation.

## Service errors and shutdown

`GrpcExchangeService` maps input before submitting it to the queue, then completes
the RPC from the returned future. Successful RPCs include ordinary business rejections.

| Condition | gRPC result |
| --- | --- |
| Valid place/cancel, including business rejection | `SubmitResponse` |
| Invalid UUID, absent command, invalid side, or nonpositive price/quantity | `INVALID_ARGUMENT` |
| Gateway admission failure or Aeron request/reply failure | `UNAVAILABLE`, with the request UUID and instructions to retry the same UUID and command |
| Unexpected response conversion failure | `INTERNAL` |

The gateway currently maps all admission failures to `UNAVAILABLE`; distinguishing
queue exhaustion from a stopped gateway needs typed failure categories. A deadline
or cancelled RPC does not undo an accepted engine command.

Stop the gateway with **⌃C** before stopping the engine. The shutdown hook stops new
RPCs and gives existing calls ten seconds to finish, then requests forced gRPC
shutdown. Main closes the gateway and drains accepted commands before closing the
borrowed Aeron resources. The hook waits for main to finish; ten seconds is the gRPC
grace period, not a bound on the complete queue drain.

## Future Go integration

The Go backend will consume a versioned copy of this contract and generate its
client bindings. Its Go module/import path has not been chosen yet, so the schema
does not declare a `go_package`. When adding Go generation, supply the same import
mapping to both `protoc-gen-go` and `protoc-gen-go-grpc`, or establish a shared
`go_package` then. See the [Go generated code guide](https://protobuf.dev/reference/go/go-generated/#packages).

The Java gateway lives in this repository and runs in a separate JVM from
`AeronEngineServer`. The request path is:

```text
GrpcGatewayClient (or a future Go gRPC client)
  -> GrpcExchangeService: protobuf to CommandRequest
  -> EngineGateway.submit: bounded queue and CompletableFuture
  -> AeronRequestClient: publication and response polling
  -> existing engine and Archive
  -> future completes -> protobuf SubmitResponse -> gRPC client
```

This does not replace the SBE Archive format or the existing live Aeron text codecs.

## Tests

`ExchangeProtocolTest` reads independent protobuf wire fixtures for both commands
and every result variant. The fixtures cover values beyond floating-point integer
precision, signed 64-bit limits, trade ordering, and a cancel result whose fields
have default values but whose `oneof` is explicitly set. These protect the external
contract from accidental field renumbering or integer narrowing. They do not require
one canonical byte ordering for equivalent protobuf messages.


`GrpcCommandMapperTest` covers domain validation, signed-long preservation, result
variants, and trade field/order mapping. `GrpcExchangeServiceTest` makes real TCP
gRPC calls through the service and gateway worker, covering successful results,
UUID retries without a second fill, business rejections, invalid input, transport
failure, a closed gateway, and response conversion failure. These focused service
tests replace the Aeron send operation with the real state machine or a controlled
failure; the existing Aeron/Archive integration tests cover transport and persistence.

The demo is a local plaintext integration without authentication or TLS. The Go
backend, deployment security, and a bounded total shutdown policy remain future work.
