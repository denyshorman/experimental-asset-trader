package com.gitlab.dhorman.cryptotrader.core

import io.circe.{Decoder, Encoder, KeyDecoder, KeyEncoder}

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
}
