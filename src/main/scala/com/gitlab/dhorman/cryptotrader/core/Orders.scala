package com.gitlab.dhorman.cryptotrader.core

import com.gitlab.dhorman.cryptotrader.core.Prices._
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import scala.util.control.Breaks._

object Orders {
  def getInstantOrder(
    market: Market,
    targetCurrency: Currency,
    initCurrencyAmount: Amount,
    takerFeeMultiplier: BigDecimal,
    orderBook: OrderBook,
  ): Option[InstantOrder] = {
    for {
      fromCurrency <- market.other(targetCurrency)
      orderTpe <- market.orderType(targetCurrency)
    } yield {
      var trades: List[InstantOrder.Trade] = Nil
      var unusedFromCurrencyAmount: Amount = initCurrencyAmount
      var targetCurrencyAmount: BigDecimal = 0

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
      }

      targetCurrencyAmount *= takerFeeMultiplier

      InstantOrder(
        market,
        fromCurrency,
        targetCurrency,
        initCurrencyAmount,
        targetCurrencyAmount,
        orderTpe,
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
    orderBook: OrderBook,
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
    orderBook: OrderBook,
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

  case class InstantOrder(
    market: Market,
    fromCurrency: Currency,
    targetCurrency: Currency,
    fromCurrencyAmount: Amount,
    targetCurrencyAmount: Amount,
    orderType: OrderType,
    unusedFromCurrencyAmount: Amount,
    feeMultiplier: BigDecimal,
    trades: List[InstantOrder.Trade],
  )

  case class DelayedOrder(
    market: Market,
    fromCurrency: Currency,
    targetCurrency: Currency,
    fromAmount: Amount,
    basePrice: Price,
    quoteAmount: Amount,
    toAmount: Amount,
    orderTpe: OrderType,
    orderMultiplier: BigDecimal,
    stat: TradeStatOrder,
  )

  object InstantOrder {
    implicit val encoder: Encoder[InstantOrder] = deriveEncoder

    case class Trade(
      price: Price,
      amount: Amount,
    )

    object Trade {
      implicit val encoder: Encoder[Trade] = deriveEncoder
    }
  }
}
