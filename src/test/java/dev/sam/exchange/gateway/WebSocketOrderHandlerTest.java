package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.Trade;
import dev.sam.exchange.transport.CommandRequest;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

class WebSocketOrderHandlerTest {
  @Test
  void rejectsAMissingSubmissionFunction() {
    assertThrows(NullPointerException.class, () -> new WebSocketOrderHandler(null, 1));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
  void rejectsNonPositivePendingLimits(int maxPending) {
    assertThrows(IllegalArgumentException.class,
        () -> new WebSocketOrderHandler(request -> new CompletableFuture<>(), maxPending));
  }

  @Test
  void rejectsMissingConfiguredLimits() {
    assertThrows(NullPointerException.class,
        () -> new WebSocketOrderHandler(request -> new CompletableFuture<>(), (WebSocketLimits) null));
  }

  @Test
  void completionsAreCorrelatedAndRoutedOnlyToTheirOwnSocket() {
    List<CompletableFuture<CommandResult>> results = new ArrayList<>();
    List<CommandRequest> requests = new ArrayList<>();
    var submit = (java.util.function.Function<CommandRequest, CompletableFuture<CommandResult>>) request -> {
      requests.add(request);
      var future = new CompletableFuture<CommandResult>();
      results.add(future);
      return future;
    };
    EmbeddedChannel first = new EmbeddedChannel(new WebSocketOrderHandler(submit, 2));
    EmbeddedChannel second = new EmbeddedChannel(new WebSocketOrderHandler(submit, 2));
    try {
      first.writeInbound(new TextWebSocketFrame(cancel(1)));
      first.writeInbound(new TextWebSocketFrame(cancel(2)));
      second.writeInbound(new TextWebSocketFrame(cancel(3)));
      results.get(2).complete(new CancelResult(3, true));
      results.get(1).complete(new CancelResult(2, false));
      first.runPendingTasks();
      second.runPendingTasks();
      assertEquals(id(2), response(first).get("requestId").getAsString());
      assertEquals(id(3), response(second).get("requestId").getAsString());
      assertNull(second.readOutbound());
      results.get(0).complete(new CancelResult(1, false));
      first.runPendingTasks();
      assertEquals(id(1), response(first).get("requestId").getAsString());
      assertEquals(List.of(1L, 2L, 3L), requests.stream().map(r -> r.command().orderId()).toList());
    } finally {
      first.finishAndReleaseAll();
      second.finishAndReleaseAll();
    }
  }

  @Test
  void boundsOutstandingRequestsWithoutPublishingTheExcessRequest() {
    var pending = new CompletableFuture<CommandResult>();
    List<CommandRequest> admitted = new ArrayList<>();
    EmbeddedChannel channel = new EmbeddedChannel(new WebSocketOrderHandler(request -> {
      admitted.add(request);
      return pending;
    }, 1));
    try {
      channel.writeInbound(new TextWebSocketFrame(cancel(1)));
      channel.writeInbound(new TextWebSocketFrame(cancel(2)));
      var reply = response(channel);
      assertEquals(id(2), reply.get("requestId").getAsString());
      assertEquals("RESOURCE_EXHAUSTED", reply.getAsJsonObject("error").get("code").getAsString());
      assertFalse(reply.getAsJsonObject("error").get("outcomeUnknown").getAsBoolean());
      assertEquals(1, admitted.size());
      pending.complete(new CancelResult(1, false));
      channel.runPendingTasks();
      response(channel);
      channel.writeInbound(new TextWebSocketFrame(cancel(3)));
      channel.runPendingTasks();
      assertEquals(2, admitted.size());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void disconnectDoesNotCancelAnAdmittedCommand() {
    var pending = new CompletableFuture<CommandResult>();
    EmbeddedChannel channel = new EmbeddedChannel(new WebSocketOrderHandler(request -> pending, 1));
    channel.writeInbound(new TextWebSocketFrame(cancel(1)));
    channel.close();
    assertFalse(pending.isCancelled());
    pending.complete(new CancelResult(1, true));
    channel.runPendingTasks();
    assertNull(channel.readOutbound());
    channel.finishAndReleaseAll();
  }

  @Test
  void malformedInputNeverReachesTheGatewayAndDoesNotPoisonTheConnection() {
    List<CommandRequest> admitted = new ArrayList<>();
    EmbeddedChannel channel = new EmbeddedChannel(new WebSocketOrderHandler(request -> {
      admitted.add(request);
      return CompletableFuture.completedFuture(new CancelResult(1, false));
    }, 1));
    try {
      channel.writeInbound(new TextWebSocketFrame("{}"));
      assertEquals("INVALID_ARGUMENT", response(channel).getAsJsonObject("error").get("code").getAsString());
      assertTrue(admitted.isEmpty());
      channel.writeInbound(new TextWebSocketFrame(cancel(1)));
      channel.runPendingTasks();
      assertTrue(response(channel).has("cancel"));
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void transportFailureReportsUnknownOutcomeAndRetryIdentity() {
    EmbeddedChannel channel = new EmbeddedChannel(new WebSocketOrderHandler(
        request -> CompletableFuture.failedFuture(new IllegalStateException("lost reply")), 1));
    try {
      channel.writeInbound(new TextWebSocketFrame(cancel(1)));
      channel.runPendingTasks();
      var reply = response(channel);
      assertEquals(id(1), reply.get("requestId").getAsString());
      assertTrue(reply.getAsJsonObject("error").get("outcomeUnknown").getAsBoolean());
      assertEquals("UNAVAILABLE", reply.getAsJsonObject("error").get("code").getAsString());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void aFailedCompletionReleasesCapacityForTheNextCommand() {
    List<CompletableFuture<CommandResult>> results = new ArrayList<>();
    EmbeddedChannel channel = new EmbeddedChannel(new WebSocketOrderHandler(request -> {
      var result = new CompletableFuture<CommandResult>();
      results.add(result);
      return result;
    }, 1));
    try {
      channel.writeInbound(new TextWebSocketFrame(cancel(1)));
      channel.writeInbound(new TextWebSocketFrame(cancel(2)));
      assertEquals("RESOURCE_EXHAUSTED", response(channel).getAsJsonObject("error").get("code").getAsString());
      results.getFirst().completeExceptionally(new IllegalStateException("lost reply"));
      channel.runPendingTasks();
      assertEquals("UNAVAILABLE", response(channel).getAsJsonObject("error").get("code").getAsString());
      channel.writeInbound(new TextWebSocketFrame(cancel(3)));
      assertEquals(2, results.size());
      results.getLast().complete(new CancelResult(3, false));
      channel.runPendingTasks();
      var reply = response(channel);
      assertEquals(id(3), reply.get("requestId").getAsString());
      assertEquals("3", reply.getAsJsonObject("cancel").get("orderId").getAsString());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void synchronousSubmissionFailureReleasesCapacityForTheNextCommand() {
    EmbeddedChannel channel = new EmbeddedChannel(new WebSocketOrderHandler(request -> {
      if (request.command().orderId() == 1)
        throw new RejectedExecutionException("not running");
      return CompletableFuture.completedFuture(new CancelResult(request.command().orderId(), false));
    }, 1));
    try {
      channel.writeInbound(new TextWebSocketFrame(cancel(1)));
      var failure = response(channel);
      assertEquals("UNAVAILABLE", failure.getAsJsonObject("error").get("code").getAsString());
      assertFalse(failure.getAsJsonObject("error").get("outcomeUnknown").getAsBoolean());
      channel.writeInbound(new TextWebSocketFrame(cancel(2)));
      channel.runPendingTasks();
      var reply = response(channel);
      assertEquals(id(2), reply.get("requestId").getAsString());
      assertEquals("2", reply.getAsJsonObject("cancel").get("orderId").getAsString());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void unavailableAdmissionIsKnownNotToHaveBeenAccepted() {
    EmbeddedChannel channel = new EmbeddedChannel(new WebSocketOrderHandler(
        request -> CompletableFuture.failedFuture(new RejectedExecutionException("not running")), 1));
    try {
      channel.writeInbound(new TextWebSocketFrame(cancel(1)));
      channel.runPendingTasks();
      assertFalse(response(channel).getAsJsonObject("error").get("outcomeUnknown").getAsBoolean());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void sharedQueueOverflowReportsKnownAdmissionFailure() {
    EmbeddedChannel channel = new EmbeddedChannel(new WebSocketOrderHandler(
        request -> CompletableFuture.failedFuture(new GatewayOverloadedException("queue full")), 1));
    try {
      channel.writeInbound(new TextWebSocketFrame(cancel(1)));
      channel.runPendingTasks();
      var reply = response(channel);
      assertEquals(id(1), reply.get("requestId").getAsString());
      assertEquals("RESOURCE_EXHAUSTED", reply.getAsJsonObject("error").get("code").getAsString());
      assertFalse(reply.getAsJsonObject("error").get("outcomeUnknown").getAsBoolean());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void binaryFramesCloseTheConnectionWithoutSubmitting() {
    EmbeddedChannel channel = new EmbeddedChannel(new WebSocketOrderHandler(request -> {
      fail("Binary input must not submit");
      return null;
    }, 1));
    try {
      channel.writeInbound(new BinaryWebSocketFrame());
      CloseWebSocketFrame closed = channel.readOutbound();
      assertEquals(1003, closed.statusCode());
      closed.release();
      assertFalse(channel.isActive());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void anUnwritableConnectionClosesInsteadOfAccumulatingMoreReplies() {
    EmbeddedChannel channel = new EmbeddedChannel(new WebSocketOrderHandler(request -> {
      fail("Slow connection must not submit");
      return null;
    }, 1));
    try {
      channel.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
      channel.runPendingTasks();
      assertFalse(channel.isActive());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void configuredResponseBudgetCanExceedTheLegacyDefault() {
    var result = new PlaceResult(1, Collections.nCopies(16384, new Trade(1, 2, 3, 4)), 0);
    var limits = new WebSocketLimits(1, 1, 16384, 2097152, 60000, 5000);
    EmbeddedChannel channel = new EmbeddedChannel(
        new WebSocketOrderHandler(request -> CompletableFuture.completedFuture(result), limits));
    // Keep this response-budget check independent of the separate slow-reader watermark.
    channel.config().setWriteBufferWaterMark(new WriteBufferWaterMark(2097152, 3145728));
    try {
      channel.writeInbound(new TextWebSocketFrame(cancel(1)));
      channel.runPendingTasks();
      var reply = response(channel);
      assertEquals(16384, reply.getAsJsonObject("place").getAsJsonArray("trades").size());
      assertTrue(channel.isActive());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void legacyHandlerRetainsTheDefaultResponseBudget() {
    var result = new PlaceResult(1, Collections.nCopies(16384, new Trade(1, 2, 3, 4)), 0);
    EmbeddedChannel channel = new EmbeddedChannel(
        new WebSocketOrderHandler(request -> CompletableFuture.completedFuture(result), 1));
    try {
      channel.writeInbound(new TextWebSocketFrame(cancel(1)));
      channel.runPendingTasks();
      CloseWebSocketFrame close = assertInstanceOf(CloseWebSocketFrame.class, channel.readOutbound());
      try {
        assertEquals(1009, close.statusCode());
        assertTrue(close.reasonText().contains("outcome unknown"));
      } finally {
        close.release();
      }
      assertFalse(channel.isActive());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  static String id(long n) {
    return new UUID(0, n).toString();
  }
  static String cancel(long n) {
    return "{\"requestId\":\"" + id(n) + "\",\"cancel\":{\"orderId\":\"" + n + "\"}}";
  }
  private static JsonObject response(EmbeddedChannel channel) {
    TextWebSocketFrame frame = channel.readOutbound();
    assertNotNull(frame);
    try {
      return JsonParser.parseString(frame.text()).getAsJsonObject();
    } finally {
      frame.release();
    }
  }
}
