package com.gitlab.dhorman.cryptotrader.core

import org.scalatest.FlatSpec
import io.circe.syntax._
import scala.collection.immutable.TreeMap

class MarketTest extends FlatSpec {
  "Market" should "generate correct trades USDT => BTC" in {
    val market: Market = "USDT_BTC"

    val orderBook = OrderBook(
      asks = TreeMap[BigDecimal, BigDecimal](
        BigDecimal(50) -> BigDecimal(1),
        BigDecimal(60) -> BigDecimal(0.2),
      ),
      bids = TreeMap(
        BigDecimal(40) -> BigDecimal(1),
        BigDecimal(30) -> BigDecimal(2),
      )(implicitly[Ordering[Price]].reverse),
    )

    val result = Orders.getInstantOrder(
      market,
      "BTC",
      100,
      0.998,
      orderBook
    )

    println(result.get.asJson.spaces2)
  }

  "Market" should "generate correct trades BTC => USDT" in {
    val market: Market = "USDT_BTC"

    val orderBook = OrderBook(
      asks = TreeMap[BigDecimal, BigDecimal](
        BigDecimal(50) -> BigDecimal(1),
        BigDecimal(60) -> BigDecimal(0.2),
      ),
      bids = TreeMap(
        BigDecimal(40) -> BigDecimal(1),
        BigDecimal(30) -> BigDecimal(2),
      )(implicitly[Ordering[Price]].reverse),
    )

    val result = Orders.getInstantOrder(
      market,
      "USDT",
      1.2,
      0.998,
      orderBook
    )

    println(result.get.asJson.spaces2)
  }
}
