package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.cache.service

import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexfutures.PoloniexFuturesApi
import com.gitlab.dhorman.cryptotrader.util.infiniteRetry
import com.gitlab.dhorman.cryptotrader.util.subscribed
import com.gitlab.dhorman.cryptotrader.util.transformFirst
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.shareIn
import mu.KotlinLogging
import kotlin.time.hours

class CacheablePoloniexFuturesApi(val api: PoloniexFuturesApi) : AutoCloseable {
    private val logger = KotlinLogging.logger {}
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob() + CoroutineName("CacheablePoloniexFuturesApi"))

    override fun close() {
        scope.cancel()
    }

    val openContracts: Flow<Map<String, PoloniexFuturesApi.ContractInfo>> = run {
        flow {
            while (currentCoroutineContext().isActive) {
                logger.info("Fetching open contracts...")
                val openContracts = infiniteRetry { api.getOpenContracts() }
                logger.info("Open contracts has been fetched")
                val openContractsMap = openContracts.asSequence().map { it.symbol to it }.toMap()
                emit(openContractsMap)
                delay(6.hours)
            }
        }.shareIn(scope, SharingStarted.Lazily, 1)
    }

    val privateMessagesStream = run {
        api.privateMessagesStream
            .shareIn(scope, SharingStarted.Eagerly, 1)
            .transformFirst { if (it.subscribed) emit(it.subscribed()) }
    }
}
