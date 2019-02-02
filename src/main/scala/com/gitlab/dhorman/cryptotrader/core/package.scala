package com.gitlab.dhorman.cryptotrader

import java.time.Instant

import io.circe._

import scala.collection.immutable.TreeMap

package object core {
  type Currency = String
  type Price = BigDecimal
  type Amount = BigDecimal

  type CurrencyType = CurrencyType.Value
  object CurrencyType extends Enumeration {
    val Base: CurrencyType = Value
    val Quote: CurrencyType = Value
  }

  type OrderType = OrderType.Value
  object OrderType extends Enumeration {
    val Sell: OrderType = Value
    val Buy: OrderType = Value

    implicit val encoder: Encoder[OrderType] = Encoder.encodeString.contramap {
      case Sell => "SELL"
      case Buy => "BUY"
    }

    implicit val decoder: Decoder[OrderType] = Decoder.decodeJson.emap(json => {
      if (json.isString) {
        json.asString.get.toUpperCase match {
          case "SELL" => Right(Sell)
          case "BUY" => Right(Buy)
          case str => Left(s"""Not recognized string "$str" """)
        }
      } else if (json.isNumber) {
        json.asNumber.get.toInt.get match {
          case 0 => Right(Sell)
          case 1 => Right(Buy)
          case int => Left(s"""Not recognized integer "$int" """)
        }
      } else {
        Left("Not recognized OrderType")
      }
    })
  }

  type SubOrderBook = TreeMap[Price, Amount]

  case class OrderBook(
    asks: SubOrderBook = TreeMap(),
    bids: SubOrderBook = TreeMap()(implicitly[Ordering[Price]].reverse),
  )

  case class FeeMultiplier(maker: BigDecimal, taker: BigDecimal)

  object FeeMultiplier {
    def from(makerFeePercent: BigDecimal, takerFeePercent: BigDecimal): FeeMultiplier = {
      FeeMultiplier(1 - makerFeePercent, 1 - takerFeePercent)
    }
  }

  case class TradeStat(
    sell: TradeStatOrder,
    buy: TradeStatOrder,
  )

  case class TradeStatOrder(
    ttwAvgMs: Long,
    ttwVariance: Long,
    ttwStdDev: Long,
    minAmount: BigDecimal,
    maxAmount: BigDecimal,
    avgAmount: BigDecimal,
    varianceAmount: BigDecimal,
    stdDevAmount: BigDecimal,
    firstTranTs: Instant,
    lastTranTs: Instant,
  )
}
