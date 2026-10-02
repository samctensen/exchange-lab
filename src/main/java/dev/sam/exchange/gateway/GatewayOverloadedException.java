package dev.sam.exchange.gateway;

import java.util.concurrent.RejectedExecutionException;

/** The command was rejected before entering the gateway queue. */
public final class GatewayOverloadedException extends RejectedExecutionException {
  private static final long serialVersionUID = 1L;

  public GatewayOverloadedException(String message) {
    super(message);
  }
}
