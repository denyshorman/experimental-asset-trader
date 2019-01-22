package com.gitlab.dhorman.cryptotrader.core

import io.circe.{Decoder, Encoder, KeyDecoder, KeyEncoder}
import io.circe.generic.semiauto.deriveEncoder

import scala.annotation.tailrec
import PriceUtil._

case class Market(baseCurrency: Currency, quoteCurrency: Currency) {
  def b: Currency = baseCurrency
  def q: Currency = quoteCurrency

  def tpe(currency: Currency): Option[CurrencyType] = {
    if (currency == baseCurrency) return Some(CurrencyType.Base)
    if (currency == quoteCurrency) return Some(CurrencyType.Quote)
    None
  }

  def currency(tpe: CurrencyType): Currency = tpe match {
    case CurrencyType.Base => baseCurrency
    case CurrencyType.Quote => quoteCurrency
  }

  def other(currency: Currency): Option[Currency] = {
    if (currency == baseCurrency) return Some(quoteCurrency)
    if (currency == quoteCurrency) return Some(baseCurrency)
    None
  }

  def orderType(targetCurrency: Currency): Option[OrderType] = {
    tpe(targetCurrency) map orderType
  }

  def orderType(currencyType: CurrencyType): OrderType = currencyType match {
    case CurrencyType.Base => OrderType.Sell
    case CurrencyType.Quote => OrderType.Buy
  }

  def contains(currency: Currency): Boolean = {
    currency == baseCurrency || currency == quoteCurrency
  }

  def find(currencies: Iterable[Currency]): Option[Currency] = {
    currencies.find(currency => currency == baseCurrency || currency == quoteCurrency)
  }

  def get(
    targetCurrency: Currency,
    price: Price,
    fromCurrencyAmount: Amount = 1,
    feeMultiplier: BigDecimal = 1,
  ): Option[BigDecimal] = orderType(targetCurrency) map { orderTpe =>
    get(orderTpe, price, fromCurrencyAmount, feeMultiplier)
  }

  def get(
    orderTpe: OrderType,
    price: Price,
    amount: Amount,
    feeMultiplier: BigDecimal,
  ): BigDecimal = orderTpe match {
    case OrderType.Buy => (amount / price) * feeMultiplier
    case OrderType.Sell => (amount * price) * feeMultiplier
  }

  def getInstantOrder(
    targetCurrency: Currency,
    initCurrencyAmount: Amount,
    takerFeeMultiplier: BigDecimal,
    orderBook: OrderBook,
  ): Option[Market.InstantOrder] = {
    for {
      fromCurrency <- other(targetCurrency)
      orderTpe <- orderType(targetCurrency)
    } yield {
      val (unusedFromCurrencyAmount, targetCurrencyAmount, trades) = getInstantTrades(
        fromAmount = initCurrencyAmount,
        subBook = getInstantSubOrderBook(orderBook, orderTpe),
        orderTpe = orderTpe,
        takerFeeMultiplier = takerFeeMultiplier,
      )

      Market.InstantOrder(
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
    targetCurrency: Currency,
    fromAmount: Amount,
    makerFeeMultiplier: BigDecimal,
    orderBook: OrderBook,
    lastTrades: List[Trade],
  ): Option[Market.DelayedOrder] = {
    for {
      fromCurrency <- other(targetCurrency)
      orderTpe <- orderType(targetCurrency)
    } yield {
      orderTpe match {
        case OrderType.Buy =>
          val subMarket = orderBook.bids
          val basePrice = subMarket.head._1.cut8add1
          val quoteAmount = fromAmount / basePrice
          val toAmount = quoteAmount * makerFeeMultiplier
          val avgWaitTimeSec = 60 // TODO: calc avg time

          Market.DelayedOrder(
            fromCurrency,
            targetCurrency,
            fromAmount,
            basePrice,
            quoteAmount,
            toAmount,
            orderTpe,
            avgWaitTimeSec,
          )
        case OrderType.Sell =>
          val subMarket = orderBook.asks
          val basePrice = subMarket.head._1.cut8add1
          val quoteAmount = fromAmount
          val toAmount = quoteAmount * basePrice * makerFeeMultiplier
          val avgWaitTimeSec = 60 // TODO: calc avg time

          Market.DelayedOrder(
            fromCurrency,
            targetCurrency,
            fromAmount,
            basePrice,
            quoteAmount,
            toAmount,
            orderTpe,
            avgWaitTimeSec,
          )
      }
    }
  }

  private def getInstantSubOrderBook(orderBook: OrderBook, orderTpe: OrderType): SubOrderBook = {
    orderTpe match {
      case OrderType.Buy => orderBook.asks
      case OrderType.Sell => orderBook.bids
    }
  }

  @tailrec
  private def getInstantTrades(
    fromAmount: Amount,
    targetAmount: Amount = 0,
    subBook: SubOrderBook,
    orderTpe: OrderType,
    takerFeeMultiplier: BigDecimal,
    trades: List[Market.InstantOrder.Trade] = Nil
  ): (Amount, Amount, List[Market.InstantOrder.Trade]) = {
    if (subBook.isEmpty) return (fromAmount, targetAmount, trades)
    val (basePrice, quoteAmount) = subBook.head

    val availableAmount = orderTpe match {
      case OrderType.Buy => quoteAmount * basePrice
      case OrderType.Sell => quoteAmount
    }

    if (fromAmount <= availableAmount) {
      val fromBalance = 0
      val targetBalance = targetAmount + get(orderTpe, basePrice, fromAmount, takerFeeMultiplier)
      val tradeAmount = orderTpe match {
        case OrderType.Buy => fromAmount / basePrice
        case OrderType.Sell => fromAmount
      }
      val newTrades = Market.InstantOrder.Trade(basePrice, tradeAmount) :: trades
      (fromBalance, targetBalance, newTrades)
    } else {
      val fromBalance = fromAmount - availableAmount
      val toBalance = targetAmount + get(orderTpe, basePrice, availableAmount, takerFeeMultiplier)
      val trade = Market.InstantOrder.Trade(basePrice, quoteAmount)
      getInstantTrades(fromBalance, toBalance, subBook.tail, orderTpe, takerFeeMultiplier, trade :: trades)
    }
  }

  override def toString: Currency = s"${baseCurrency}_$quoteCurrency"
}

object Market {
  implicit val encoder: Encoder[Market] = Encoder.encodeString.contramap(_.toString)

  implicit val decoder: Decoder[Market] = Decoder.decodeString.emap(market => {
    val currencies = market.split('_')

    if (currencies.length == 2) {
      Right(Market(currencies(0), currencies(1)))
    } else {
      Left(s"""Market "$market" not recognized""")
    }
  })

  implicit val keyEncoder: KeyEncoder[Market] = KeyEncoder.encodeKeyString.contramap(_.toString)
  implicit val keyDecoder: KeyDecoder[Market] = KeyDecoder.decodeKeyString.map(convert)

  implicit def convert(markets: Iterable[String]): Iterable[Market] = {
    markets.map(_.split('_')).filter(_.length == 2).map(c => Market(c(0), c(1)))
  }

  implicit def convert(market: String): Market = {
    val currencies = market.split('_')
    require(currencies.length == 2, "market must be in format A_B.")
    Market(currencies(0), currencies(1))
  }

  case class InstantOrder(
    fromCurrency: Currency,
    targetCurrency: Currency,
    fromCurrencyAmount: Amount,
    targetCurrencyAmount: Amount,
    orderType: OrderType,
    unusedFromCurrencyAmount: Amount,
    feeMultiplier: BigDecimal,
    trades: List[InstantOrder.Trade],
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

  case class DelayedOrder(
    fromCurrency: Currency,
    targetCurrency: Currency,
    fromAmount: Amount,
    basePrice: Price,
    quoteAmount: Amount,
    toAmount: Amount,
    orderTpe: OrderType,
    avgWaitTimeSec: Long,
  )
}
