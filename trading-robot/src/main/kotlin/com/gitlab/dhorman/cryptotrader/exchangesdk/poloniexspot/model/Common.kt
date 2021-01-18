package com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.core.OrderBookAbstract
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.codec.OrderTypeJsonCodec
import io.vavr.collection.SortedMap
import io.vavr.collection.TreeMap
import java.math.BigDecimal
import java.time.Instant
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

typealias MarketId = Int
typealias Currency = String
typealias Price = BigDecimal
typealias Amount = BigDecimal
typealias SubOrderBook = SortedMap<Price, Amount>

enum class CurrencyType {
    Base,
    Quote;

    operator fun not() = when (this) {
        Base -> Quote
        Quote -> Base
    }
}

@JsonSerialize(using = OrderTypeJsonCodec.Encoder::class)
@JsonDeserialize(using = OrderTypeJsonCodec.Decoder::class)
enum class OrderType {
    Sell,
    Buy;

    operator fun not() = when (this) {
        Sell -> Buy
        Buy -> Sell
    }
}

data class PriceAggregatedBook(
    override val asks: TreeMap<Price, Amount> = TreeMap.empty(),
    override val bids: TreeMap<Price, Amount> = TreeMap.empty(compareByDescending { it })
) : OrderBookAbstract(asks, bids)

fun Instant.round(period: ChartDataCandlestickPeriod): Instant {
    val time = this.atZone(ZoneOffset.UTC)

    return when (period) {
        ChartDataCandlestickPeriod.PERIOD_5_MIN,
        ChartDataCandlestickPeriod.PERIOD_15_MIN,
        ChartDataCandlestickPeriod.PERIOD_30_MIN -> {
            val newMin = (time.minute / period.min) * period.min
            time.truncatedTo(ChronoUnit.HOURS).withMinute(newMin).toInstant()
        }
        ChartDataCandlestickPeriod.PERIOD_2_HOURS,
        ChartDataCandlestickPeriod.PERIOD_4_HOURS,
        ChartDataCandlestickPeriod.PERIOD_DAY -> {
            val newHour = (time.hour / period.hour) * period.hour
            time.truncatedTo(ChronoUnit.DAYS).withHour(newHour).toInstant()
        }
    }
}
