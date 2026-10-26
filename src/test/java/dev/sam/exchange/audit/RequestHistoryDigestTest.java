package dev.sam.exchange.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.transport.CommandRequest;

class RequestHistoryDigestTest {
  private static final UUID PLACE_ID = UUID.fromString("01234567-89ab-cdef-fedc-ba9876543210");
  private static final UUID CANCEL_ID = UUID.fromString("8899aabb-ccdd-eeff-0011-223344556677");
  private static final CommandRequest PLACE = new CommandRequest(PLACE_ID,
      new PlaceOrder(Long.MIN_VALUE, Side.ASK, 101L, 7L));
  private static final CommandRequest CANCEL = new CommandRequest(CANCEL_ID, new CancelOrder(Long.MIN_VALUE));

  // Independent fixtures use Python hashlib and struct, not the Java codec or digest:
  // place: struct.pack('<HHHHQQqBqq', 41, 3, 1, 0, uuid_msb, uuid_lsb, order_id, side, price, lots)
  // cancel: struct.pack('<HHHHQQq', 24, 4, 1, 0, uuid_msb, uuid_lsb, order_id)
  // h0 = sha256(b'EXCHANGE-REQUEST-HISTORY-V1\n'); hn = sha256(hprev + pack('<I', len(frame)) + frame)
  @Test
  void matchesGoldenChainWithoutConsumingSnapshots() {
    RequestHistoryDigest digest = new RequestHistoryDigest();
    assertEquals("b2f9064c93325a748e6dbbe28931d844f9c50e1612a109251001f02f45e0ec5c", digest.snapshotSha256());

    digest.append(PLACE);
    assertEquals("17010157c49d1dd73a6ed74703171d58264135d05920881a22840935dd29be26", digest.snapshotSha256());
    assertEquals("17010157c49d1dd73a6ed74703171d58264135d05920881a22840935dd29be26", digest.snapshotSha256());

    digest.append(CANCEL);
    assertEquals("09344e203b3154bc6176b5339204fda238200165f0d6d896755b76bd63676687", digest.snapshotSha256());
    digest.append(PLACE);
    assertEquals("c26a7292bec01ce23e8cfb493c7a55f6de89e10efba44382d7904eb7501fe1c3", digest.snapshotSha256());
  }

  @Test
  void reorderingTheSameFramesChangesTheGoldenChain() {
    RequestHistoryDigest digest = new RequestHistoryDigest();
    digest.append(CANCEL);
    digest.append(PLACE);

    assertEquals("3eb99c5a7e3a19d38a71bb95ffcf6d6a8abe721b4d0d33ecf72c2851e2a1ee3b", digest.snapshotSha256());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("changedFields")
  void fingerprintsEveryRequestField(String changedField, CommandRequest request, String expected) {
    RequestHistoryDigest digest = new RequestHistoryDigest();
    digest.append(request);

    assertEquals(expected, digest.snapshotSha256());
  }

  static Stream<Arguments> changedFields() {
    return Stream.of(
        Arguments.of("UUID most significant bits",
            new CommandRequest(UUID.fromString("11234567-89ab-cdef-fedc-ba9876543210"), PLACE.command()),
            "75312fd464e3a644ff5f57264c7fcc188331b6a3762659b87935324682a74ea4"),
        Arguments.of("UUID least significant bits",
            new CommandRequest(UUID.fromString("01234567-89ab-cdef-fedc-ba9876543211"), PLACE.command()),
            "947d67d0fb52df99e3b69e164688a99169263f7f58a3d681d8d805abc4908e3d"),
        Arguments.of("order ID", new CommandRequest(PLACE_ID, new PlaceOrder(Long.MIN_VALUE + 1, Side.ASK, 101L, 7L)),
            "a11f0f152a2400a44334bd4b5aaae168873075bc1b0c6fec0cab127c5d066980"),
        Arguments.of("side", new CommandRequest(PLACE_ID, new PlaceOrder(Long.MIN_VALUE, Side.BID, 101L, 7L)),
            "b0c7ad4d232b98c2c250cba3affba43b1019a6370943329949603bd08b602ecd"),
        Arguments.of("price", new CommandRequest(PLACE_ID, new PlaceOrder(Long.MIN_VALUE, Side.ASK, 102L, 7L)),
            "8dd197468c6a4e81a99a3583c6a0e7a068c7244bda0d43e15e5e5c802384063e"),
        Arguments.of("quantity", new CommandRequest(PLACE_ID, new PlaceOrder(Long.MIN_VALUE, Side.ASK, 101L, 8L)),
            "fe61101cc20e866b377ca1f87563a5ba466cf1a509d2bb4d114df32f081158b9"),
        Arguments.of("command type", new CommandRequest(PLACE_ID, new CancelOrder(Long.MIN_VALUE)),
            "b0a2712d4b001953531b4eef912369c0302265a8f396d05a5e512a12a9a41217"));
  }
}
