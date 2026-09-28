package dev.sam.exchange.gateway;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Function;

import org.agrona.DirectBuffer;

import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.transport.AeronRequestClient;
import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.CommandResponse;

// The real gateway owns the worker and futures; this replaces only the transport boundary.
class ReplyingAeronClient extends AeronRequestClient {
  private final SbeRequestCodec codec = new SbeRequestCodec();
  private final Queue<CommandResponse> responses = new ArrayDeque<>();
  private final Function<CommandRequest, CommandResult> process;

  ReplyingAeronClient(Function<CommandRequest, CommandResult> process) {
    super(null, null);
    this.process = process;
  }

  @Override
  public long trySend(DirectBuffer buffer, int offset, int length) {
    CommandRequest request = codec.decode(buffer, offset, length);
    responses.add(new CommandResponse(request.requestId(), process.apply(request)));
    return 128;
  }

  @Override
  public int pollResponses(Consumer<CommandResponse> onResponse, int fragmentLimit) {
    int work = 0;
    CommandResponse response;
    while (work < fragmentLimit && (response = responses.poll()) != null) {
      onResponse.accept(response);
      work++;
    }
    return work;
  }
}
