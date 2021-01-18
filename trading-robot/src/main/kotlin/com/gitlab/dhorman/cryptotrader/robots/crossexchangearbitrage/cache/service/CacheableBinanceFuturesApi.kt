package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.cache.service

import com.gitlab.dhorman.cryptotrader.service.binance.BinanceFuturesApi
import com.gitlab.dhorman.cryptotrader.util.infiniteRetry
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.shareIn
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.hours

class CacheableBinanceFuturesApi(val api: BinanceFuturesApi) : AutoCloseable {
    private val logger = KotlinLogging.logger {}
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob() + CoroutineName("CacheableBinanceFuturesApi"))
    private val commissionRateCache = ConcurrentHashMap<String, Flow<BinanceFuturesApi.CommissionRate>>()

    override fun close() {
        scope.cancel()
    }

    val exchangeInfo = run {
        flow {
            while (currentCoroutineContext().isActive) {
                logger.info("Fetching exchange info...")
                val exchangeInfo = infiniteRetry { api.getExchangeInfo() }
                logger.info("Exchange info has been fetched")
                emit(exchangeInfo)
                delay(6.hours)
            }
        }.shareIn(scope, SharingStarted.Lazily, 1)
    }

    fun getCommissionRate(symbol: String): Flow<BinanceFuturesApi.CommissionRate> {
        return commissionRateCache.getOrPut(symbol) {
            flow {
                while (currentCoroutineContext().isActive) {
                    logger.info("Fetching commission rate for $symbol")
                    val commissionRate = infiniteRetry { api.getCommissionRate(symbol) }
                    logger.info("Commission rate for $symbol has been fetched")
                    emit(commissionRate)
                    delay(6.hours)
                }
            }.shareIn(scope, SharingStarted.Lazily, 1)
        }
    }
}
