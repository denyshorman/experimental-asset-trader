package com.gitlab.dhorman.cryptotrader.service.poloniex.model

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.gitlab.dhorman.cryptotrader.core.OrderBookAbstract
import com.gitlab.dhorman.cryptotrader.service.poloniex.codec.OrderTypeJsonCodec
import io.vavr.collection.SortedMap
import io.vavr.collection.TreeMap
import java.math.BigDecimal

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
