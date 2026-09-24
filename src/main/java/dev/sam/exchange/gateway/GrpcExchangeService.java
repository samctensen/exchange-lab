package dev.sam.exchange.gateway;

import dev.sam.exchange.gateway.proto.ExchangeServiceGrpc.ExchangeServiceImplBase;
import dev.sam.exchange.gateway.proto.SubmitRequest;
import dev.sam.exchange.gateway.proto.SubmitResponse;
import dev.sam.exchange.transport.CommandRequest;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class GrpcExchangeService extends ExchangeServiceImplBase {

  private final EngineGateway gateway;
  private final GrpcCommandMapper mapper;

  public GrpcExchangeService(EngineGateway gateway, GrpcCommandMapper mapper) {
    this.gateway = gateway;
    this.mapper = mapper;
  }

  @Override
  public void submit(SubmitRequest request, StreamObserver<SubmitResponse> responseObserver) {

    final CommandRequest commandRequest;

    // Reject malformed input before it reaches the gateway.
    try {
      commandRequest = mapper.toCommandRequest(request);
    } catch (IllegalArgumentException e) {
      responseObserver
          .onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e).asRuntimeException());
      return;
    }

    gateway.submit(commandRequest).whenComplete((result, failure) -> {
      if (failure != null) {
        // The engine might have processed the command even if its reply was lost.
        responseObserver.onError(
            Status.UNAVAILABLE.withDescription("Engine response unavailable for request " + commandRequest.requestId()
                + ". Retry with the same request ID and command.").withCause(failure).asRuntimeException());
        return;
      }

      final SubmitResponse response;
      try {
        response = mapper.toSubmitResponse(commandRequest.requestId(), result);
      } catch (RuntimeException e) {
        responseObserver.onError(
            Status.INTERNAL.withDescription("Failed to convert the engine response").withCause(e).asRuntimeException());
        return;
      }

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    });
  }
}
