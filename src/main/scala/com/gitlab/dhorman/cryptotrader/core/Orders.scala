package com.gitlab.dhorman.cryptotrader.core

import com.gitlab.dhorman.cryptotrader.core.Prices._
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

import scala.annotation.tailrec

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
      val (unusedFromCurrencyAmount, targetCurrencyAmount, trades) = getInstantTrades(
        market = market,
        fromAmount = initCurrencyAmount,
        subBook = getInstantSubOrderBook(orderBook, orderTpe),
        orderTpe = orderTpe,
        takerFeeMultiplier = takerFeeMultiplier,
      )

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

  private def getInstantSubOrderBook(orderBook: OrderBook, orderTpe: OrderType): SubOrderBook = {
    orderTpe match {
      case OrderType.Buy => orderBook.asks
      case OrderType.Sell => orderBook.bids
    }
  }

  private def buyOrSell(
    orderTpe: OrderType,
    price: Price,
    amount: Amount,
    feeMultiplier: BigDecimal,
  ): BigDecimal = orderTpe match {
    case OrderType.Buy => (amount / price) * feeMultiplier
    case OrderType.Sell => (amount * price) * feeMultiplier
  }

  @tailrec
  private def getInstantTrades(
    market: Market,
    fromAmount: Amount,
    targetAmount: Amount = 0,
    subBook: SubOrderBook,
    orderTpe: OrderType,
    takerFeeMultiplier: BigDecimal,
    trades: List[InstantOrder.Trade] = Nil
  ): (Amount, Amount, List[InstantOrder.Trade]) = {
    if (subBook.isEmpty) return (fromAmount, targetAmount, trades)
    val (basePrice, quoteAmount) = subBook.head

    val availableAmount = orderTpe match {
      case OrderType.Buy => quoteAmount * basePrice
      case OrderType.Sell => quoteAmount
    }

    if (fromAmount <= availableAmount) {
      val fromBalance = 0
      val targetBalance = targetAmount + buyOrSell(orderTpe, basePrice, fromAmount, takerFeeMultiplier)
      val tradeAmount = orderTpe match {
        case OrderType.Buy => fromAmount / basePrice
        case OrderType.Sell => fromAmount
      }
      val newTrades = InstantOrder.Trade(basePrice, tradeAmount) :: trades
      (fromBalance, targetBalance, newTrades)
    } else {
      val fromBalance = fromAmount - availableAmount
      val toBalance = targetAmount + buyOrSell(orderTpe, basePrice, availableAmount, takerFeeMultiplier)
      val trade = InstantOrder.Trade(basePrice, quoteAmount)

      getInstantTrades(
        market,
        fromBalance,
        toBalance,
        subBook.tail,
        orderTpe,
        takerFeeMultiplier,
        trade :: trades
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
