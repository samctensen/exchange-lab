package dev.sam.exchange;

import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;

/**
 * Hello world!
 */
public class App {

  public static void main(String[] args) {
    PlaceOrder order = new PlaceOrder(1L, Side.BID, 101L, -5L);
    System.out.println(order.side());
    System.out.println(order.priceTicks());
  }
}
