package com.gitlab.dhorman.cryptotrader.core

import com.gitlab.dhorman.cryptotrader.core.Prices._
import scala.util.control.Breaks._

object Orders {
  def getInstantOrder(
    market: Market,
    targetCurrency: Currency,
    initCurrencyAmount: Amount,
    takerFeeMultiplier: BigDecimal,
    orderBook: OrderBookAbstract,
  ): Option[InstantOrder] = {
    for {
      fromCurrency <- market.other(targetCurrency)
      orderTpe <- market.orderType(targetCurrency)
    } yield {
      var trades: List[InstantOrder.Trade] = Nil
      var unusedFromCurrencyAmount: Amount = initCurrencyAmount
      var targetCurrencyAmount: BigDecimal = 0
      var orderMultiplierSimple: BigDecimal = 0
      var orderMultiplierAmount: BigDecimal = 0

      if (orderTpe == OrderType.Buy) {
        breakable {
          for ((basePrice, quoteAmount) <- orderBook.asks) {
            val availableAmount = quoteAmount * basePrice

            if (unusedFromCurrencyAmount <= availableAmount) {
              targetCurrencyAmount += unusedFromCurrencyAmount / basePrice
              val tradeAmount = unusedFromCurrencyAmount / basePrice
              trades = InstantOrder.Trade(basePrice, tradeAmount) :: trades
              unusedFromCurrencyAmount = 0
              break
            } else {
              unusedFromCurrencyAmount -= availableAmount
              targetCurrencyAmount += availableAmount / basePrice
              trades = InstantOrder.Trade(basePrice, quoteAmount) :: trades
            }
          }
        }

        orderMultiplierSimple = orderBook.asks.headOption.map {case (price, _) => (1/price) * takerFeeMultiplier }.getOrElse {0}
      } else {
        breakable {
          for ((basePrice, quoteAmount) <- orderBook.bids) {
            if (unusedFromCurrencyAmount <= quoteAmount) {
              targetCurrencyAmount += unusedFromCurrencyAmount * basePrice
              trades = InstantOrder.Trade(basePrice, unusedFromCurrencyAmount) :: trades
              unusedFromCurrencyAmount = 0
              break
            } else {
              unusedFromCurrencyAmount -= quoteAmount
              targetCurrencyAmount += quoteAmount * basePrice
              trades = InstantOrder.Trade(basePrice, quoteAmount) :: trades
            }
          }
        }

        orderMultiplierSimple = orderBook.bids.headOption.map {case (price, _) => price * takerFeeMultiplier}.getOrElse {0}
      }

      targetCurrencyAmount *= takerFeeMultiplier
      orderMultiplierAmount = targetCurrencyAmount / initCurrencyAmount

      InstantOrder(
        market,
        fromCurrency,
        targetCurrency,
        initCurrencyAmount,
        targetCurrencyAmount,
        orderTpe,
        orderMultiplierSimple,
        orderMultiplierAmount,
        unusedFromCurrencyAmount,
        takerFeeMultiplier,
        trades,
      )
    }
  }

  def getDelayedOrder(
    market: Market,
    targetCurrency: Currency,
    fromAmount: Amount,
    makerFeeMultiplier: BigDecimal,
    orderBook: OrderBookAbstract,
    stat: TradeStatOrder,
  ): Option[DelayedOrder] = {
    for {
      fromCurrency <- market.other(targetCurrency)
      orderTpe <- market.orderType(targetCurrency)
    } yield {
      var subMarket: Map[Price, Amount] = null
      var basePrice: Price = null
      var quoteAmount: Amount = null
      var toAmount: Amount = null
      var orderMultiplier: BigDecimal = null

      if (orderTpe == OrderType.Buy) {
        subMarket = orderBook.bids
        basePrice = subMarket.head._1.cut8add1
        quoteAmount = fromAmount / basePrice
        toAmount = quoteAmount * makerFeeMultiplier
        orderMultiplier = makerFeeMultiplier / basePrice
      } else {
        subMarket = orderBook.asks
        basePrice = subMarket.head._1.cut8add1
        quoteAmount = fromAmount
        toAmount = quoteAmount * basePrice * makerFeeMultiplier
        orderMultiplier = makerFeeMultiplier * basePrice
      }

      DelayedOrder(
        market,
        fromCurrency,
        targetCurrency,
        fromAmount,
        basePrice,
        quoteAmount,
        toAmount,
        orderTpe,
        orderMultiplier,
        stat,
      )
    }
  }

  def getDelayedOrderReverse(
    market: Market,
    targetCurrency: Currency,
    toAmount: Amount,
    makerFeeMultiplier: BigDecimal,
    orderBook: OrderBookAbstract,
    stat: TradeStatOrder,
  ): Option[DelayedOrder] = {
    for {
      fromCurrency <- market.other(targetCurrency)
      orderTpe <- market.orderType(targetCurrency)
    } yield {
      var subMarket: Map[Price, Amount] = null
      var basePrice: Price = null
      var quoteAmount: Amount = null
      var fromAmount: Amount = null
      var orderMultiplier: BigDecimal = null

      if (orderTpe == OrderType.Buy) {
        subMarket = orderBook.bids
        basePrice = subMarket.head._1.cut8add1
        quoteAmount = toAmount / makerFeeMultiplier
        fromAmount = quoteAmount * basePrice
        orderMultiplier = makerFeeMultiplier / basePrice
      } else {
        subMarket = orderBook.asks
        basePrice = subMarket.head._1.cut8add1
        quoteAmount = toAmount / (basePrice * makerFeeMultiplier)
        fromAmount = quoteAmount
        orderMultiplier = makerFeeMultiplier * basePrice
      }

      DelayedOrder(
        market,
        fromCurrency,
        targetCurrency,
        fromAmount,
        basePrice,
        quoteAmount,
        toAmount,
        orderTpe,
        orderMultiplier,
        stat,
      )
    }
  }

  sealed trait InstantDelayedOrder {
    val market: Market
    val fromCurrency: Currency
    val targetCurrency: Currency
    val fromAmount: Amount
    val toAmount: Amount
    val orderType: OrderType
  }

  case class InstantOrder(
    market: Market,
    fromCurrency: Currency,
    targetCurrency: Currency,
    fromAmount: Amount,
    toAmount: Amount,
    orderType: OrderType,
    orderMultiplierSimple: BigDecimal,
    orderMultiplierAmount: BigDecimal,
    unusedFromCurrencyAmount: Amount,
    feeMultiplier: BigDecimal,
    trades: List[InstantOrder.Trade],
  ) extends InstantDelayedOrder

  case class DelayedOrder(
    market: Market,
    fromCurrency: Currency,
    targetCurrency: Currency,
    fromAmount: Amount,
    basePrice: Price,
    quoteAmount: Amount,
    toAmount: Amount,
    orderType: OrderType,
    orderMultiplier: BigDecimal,
    stat: TradeStatOrder,
  ) extends InstantDelayedOrder

  object InstantOrder {
    case class Trade(
      price: Price,
      amount: Amount,
    )
  }
}
