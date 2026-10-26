package dev.sam.exchange.audit;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

import org.agrona.concurrent.UnsafeBuffer;

import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.transport.CommandRequest;

/**
 * Version 1 history fingerprint: h0 = SHA-256(UTF-8("EXCHANGE-REQUEST-HISTORY-V1\n")); each next hash is
 * SHA-256(previous 32 hash bytes || uint32 little-endian frame length || complete SBE request frame).
 * Frames include their SBE header. Every observation participates, including duplicates and conflicts.
 * This is an ordered-history identifier, not a signature or a hash of the resulting order book.
 */
final class RequestHistoryDigest {
  private final SbeRequestCodec codec = new SbeRequestCodec();
  private final MessageDigest sha256 = newSha256();
  private byte[] hash = sha256.digest("EXCHANGE-REQUEST-HISTORY-V1\n".getBytes(StandardCharsets.UTF_8));

  void append(CommandRequest request) {
    int length = codec.encodedLength(request);
    byte[] framedRequest = new byte[Integer.BYTES + length];
    UnsafeBuffer buffer = new UnsafeBuffer(framedRequest);
    buffer.putInt(0, length, ByteOrder.LITTLE_ENDIAN);
    codec.encode(request, buffer, Integer.BYTES);
    sha256.update(hash);
    hash = sha256.digest(framedRequest);
  }

  String snapshotSha256() {
    return HexFormat.of().formatHex(hash);
  }

  private static MessageDigest newSha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }
}
