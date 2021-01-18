package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage

import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.cache.service.CacheablePoloniexFuturesApi
import com.gitlab.dhorman.cryptotrader.service.poloniexfutures.PoloniexFuturesApi
import com.gitlab.dhorman.cryptotrader.util.EventData
import com.gitlab.dhorman.cryptotrader.util.newPayload
import io.vavr.collection.TreeMap
import io.vavr.kotlin.tuple
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.math.BigDecimal

class PoloniexFuturesMarket(
    private val cacheablePoloniexFuturesApi: CacheablePoloniexFuturesApi,
    private val market: String,
) : FuturesMarket, AutoCloseable {
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob() + CoroutineName("PoloniexFuturesMarket_$market"))

    override fun close() {
        scope.cancel()
    }

    override val orderBook: Flow<EventData<OrderBook>> = run {
        cacheablePoloniexFuturesApi.api.level2Depth50Stream(market).transform { event ->
            emit(event.newPayload(event.payload?.toOrderBook()))
        }.shareIn(scope, SharingStarted.Lazily, 1)
    }

    override val generalInfo: Flow<FuturesMarketGeneralInfo> = run {
        cacheablePoloniexFuturesApi.openContracts.map { contracts ->
            val contractInfo = contracts[market]
                ?: throw RuntimeException("Futures market $market does not exist")

            FuturesMarketGeneralInfo(
                makerFee = contractInfo.makerFeeRate,
                takerFee = contractInfo.takerFeeRate,
                minQuoteAmount = contractInfo.minOrderQty,
                baseAssetPrecision = 8,
                quotePrecision = 8,
            )
        }.shareIn(scope, SharingStarted.Lazily, 1)
    }

    override suspend fun createMarketPosition(quoteAmount: BigDecimal, positionSide: PositionSide): FuturesMarketPosition {
        val generalInfo = generalInfo.first()

        return PoloniexFuturesMarketPosition(
            cacheablePoloniexFuturesApi,
            market,
            quoteAmount,
            positionSide,
            generalInfo.minQuoteAmount,
            generalInfo.takerFee,
            generalInfo.baseAssetPrecision,
        )
    }

    companion object {
        private fun PoloniexFuturesApi.Level2DepthEvent.toOrderBook(): OrderBook {
            val asks = TreeMap.ofAll(asks.stream()) { tuple(it.price, it.qty) }
            val bids = TreeMap.ofAll(compareByDescending { it }, bids.stream()) { tuple(it.price, it.qty) }
            return OrderBook(asks, bids)
        }
    }
}
