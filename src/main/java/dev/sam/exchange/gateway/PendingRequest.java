package dev.sam.exchange.gateway;

import java.util.concurrent.CompletableFuture;

import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.transport.CommandRequest;

record PendingRequest(CommandRequest request, CompletableFuture<CommandResult> result) {
}
