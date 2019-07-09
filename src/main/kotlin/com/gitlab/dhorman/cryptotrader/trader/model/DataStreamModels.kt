package com.gitlab.dhorman.cryptotrader.trader.model

import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.MarketId
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderBookNotification
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.PriceAggregatedBook
import io.vavr.Tuple2
import io.vavr.collection.Map
import kotlinx.coroutines.flow.Flow

typealias MarketIntMap = Map<MarketId, Market>
typealias MarketStringMap = Map<Market, MarketId>
typealias MarketData = Tuple2<MarketIntMap, MarketStringMap>

data class OrderBookData(
    val market: Market,
    val marketId: MarketId,
    val book: PriceAggregatedBook,
    val notification: OrderBookNotification
)
typealias OrderBookDataMap = Map<MarketId, Flow<OrderBookData>>
