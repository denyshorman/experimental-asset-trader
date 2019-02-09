package com.gitlab.dhorman.cryptotrader.core

import com.gitlab.dhorman.cryptotrader.trader.Trader.TradeStatModels.Trade2State
import com.typesafe.scalalogging.Logger
import org.scalatest.FlatSpec
import io.circe.generic.auto._
import io.circe.syntax._

import scala.collection.immutable.TreeMap
import scala.math.BigDecimal.RoundingMode

class OrdersTest extends FlatSpec {
  private val logger = Logger[OrdersTest]

  "Orders getInstantOrder BUY" should "generate correct trades USDT => BTC" in {
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

    val fromAmount = 100
    val toAmount = 1.1976
    val fee = 0.998

    val actual = Orders.getInstantOrder(
      market,
      market.quoteCurrency,
      fromAmount,
      fee,
      orderBook
    ).get

    val expected = Orders.InstantOrder(
      market,
      market.baseCurrency,
      market.quoteCurrency,
      fromAmount,
      toAmount,
      OrderType.Buy,
      0.01996,
      0.011976,
      38,
      fee,
      List(
        Orders.InstantOrder.Trade(60, 0.2),
        Orders.InstantOrder.Trade(50, 1),
      )
    )

    logger.info(
      s"""
         |USDT -> BTC
         |${actual.asJson.spaces2}
       """.stripMargin)

    assert(actual == expected)
  }

  "Orders getInstantOrder SELL" should "generate correct trades BTC => USDT" in {
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

    val fromAmount = BigDecimal(1.2)
    val toAmount = BigDecimal(45.9080)
    val fee = 0.998

    val actual = Orders.getInstantOrder(
      market,
      market.baseCurrency,
      fromAmount,
      fee,
      orderBook
    ).get

    val expected = Orders.InstantOrder(
      market,
      market.quoteCurrency,
      market.baseCurrency,
      fromAmount,
      toAmount,
      OrderType.Sell,
      39.920,
      toAmount/fromAmount,
      0,
      fee,
      List(
        Orders.InstantOrder.Trade(30, 0.2),
        Orders.InstantOrder.Trade(40, 1),
      )
    )

    logger.info(
      s"""
         |BTC -> USDT
         |${actual.asJson.spaces2}
       """.stripMargin)

    assert(actual == expected)
  }

  "Orders getDelayedOrderReverse BUY" should "generate correct fromAmount and toAmount amounts" in {
    val market: Market = "USD_UAH"
    val orderBook = OrderBook(
      asks = TreeMap[BigDecimal, BigDecimal](
        1/BigDecimal(26) -> BigDecimal(5000),
      ),
      bids = TreeMap(
        1/BigDecimal(25) -> BigDecimal(6000),
      )(implicitly[Ordering[Price]].reverse),
    )
    val makerFeeMultiplier = BigDecimal(0.999)
    val targetCurrency = "UAH"
    val toAmount = BigDecimal(260.5)
    val stat = Trade2State.map(Trade2State.calcFull(Vector()))

    val buyQuoteReverse = Orders.getDelayedOrderReverse(
      market,
      targetCurrency,
      toAmount,
      makerFeeMultiplier,
      orderBook,
      stat,
    ).get
    val buyQuoteDirect = Orders.getDelayedOrder(
      market,
      targetCurrency,
      fromAmount = buyQuoteReverse.fromAmount,
      makerFeeMultiplier,
      orderBook,
      stat,
    ).get

    logger.info(
      s"""
         |Buy quote direct
         |${buyQuoteDirect.asJson.spaces2}
         |Buy quote reverse
         |${buyQuoteReverse.asJson.spaces2}
       """.stripMargin)

    assert(buyQuoteDirect == buyQuoteReverse)
  }

  "Orders getDelayedOrderReverse SELL" should "generate correct fromAmount and toAmount amounts" in {
    val market: Market = "USD_UAH"
    val orderBook = OrderBook(
      asks = TreeMap[BigDecimal, BigDecimal](
        1/BigDecimal(26) -> BigDecimal(5000),
      ),
      bids = TreeMap(
        1/BigDecimal(25) -> BigDecimal(6000),
      )(implicitly[Ordering[Price]].reverse),
    )
    val makerFeeMultiplier = BigDecimal(0.998)
    val targetCurrency = "USD"
    val toAmount = BigDecimal(125)
    val stat = Trade2State.map(Trade2State.calcFull(Vector()))

    val sellQuoteReverse = Orders.getDelayedOrderReverse(
      market,
      targetCurrency,
      toAmount,
      makerFeeMultiplier,
      orderBook,
      stat,
    ).get
    val sellQuoteDirect = Orders.getDelayedOrder(
      market,
      targetCurrency,
      fromAmount = sellQuoteReverse.fromAmount,
      makerFeeMultiplier,
      orderBook,
      stat,
    ).get

    logger.info(
      s"""
         |Sell quote direct
         |${sellQuoteDirect.asJson.spaces2}
         |Sell quote reverse
         |${sellQuoteReverse.asJson.spaces2}
       """.stripMargin)

    assert(sellQuoteDirect == sellQuoteReverse)
  }

  "Orders path USDT_ETH -> USDC_ETH" should "generate correct fromAmount and toAmount amounts" in {
    val marketUsdtEth: Market = "USDT_ETH"
    val marketUsdcEth: Market = "USDC_ETH"
    val orderBookUsdtEth = OrderBook(
      asks = TreeMap[BigDecimal, BigDecimal](
        BigDecimal(115) -> BigDecimal(5000),
      ),
      bids = TreeMap(
        BigDecimal(114) -> BigDecimal(6000),
      )(implicitly[Ordering[Price]].reverse),
    )
    val orderBookUsdcEth = OrderBook(
      asks = TreeMap[BigDecimal, BigDecimal](
        BigDecimal(118) -> BigDecimal(5000),
      ),
      bids = TreeMap(
        BigDecimal(117) -> BigDecimal(6000),
      )(implicitly[Ordering[Price]].reverse),
    )
    val makerFeeMultiplier = BigDecimal(0.999)
    val stat = Trade2State.map(Trade2State.calcFull(Vector()))

    val targetUsdcAmount = 87

    val sellEth = Orders.getDelayedOrderReverse(
      marketUsdcEth,
      "USDC",
      targetUsdcAmount,
      makerFeeMultiplier,
      orderBookUsdcEth,
      stat,
    ).get

    val buyEth = Orders.getDelayedOrderReverse(
      marketUsdtEth,
      "ETH",
      sellEth.fromAmount,
      makerFeeMultiplier,
      orderBookUsdtEth,
      stat,
    ).get

    logger.info(
      s"""
         |Path USDT_ETH -> USDC_ETH  =>  USDT -> USDC
         |Buy ETH on market USDT_ETH
         |${buyEth.asJson.spaces2}
         |Sell ETH on market USDC_ETH
         |${sellEth.asJson.spaces2}
       """.stripMargin)

    assert((targetUsdcAmount/buyEth.fromAmount).setScale(8, RoundingMode.FLOOR) == BigDecimal(1.03301857))
  }
}
