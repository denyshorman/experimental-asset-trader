package com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader

import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.core.Market
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.ExtendedPoloniexApi
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.getOrderBookFlowBy
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.OrderType
import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.algo.SplitTradeAlgo
import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.core.AdjustedPoloniexBuySellAmountCalculator
import io.vavr.Tuple2
import io.vavr.kotlin.tuple
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import mu.KotlinLogging
import java.time.Clock

class DelayedTradeManager(
    private val scope: CoroutineScope,
    private val splitAlgo: SplitTradeAlgo,
    private val poloniexApi: ExtendedPoloniexApi,
    private val amountCalculator: AdjustedPoloniexBuySellAmountCalculator,
    private val clock: Clock
) {
    private val processors = hashMapOf<Tuple2<Market, OrderType>, DelayedTradeProcessor>()
    private val mutex = Mutex()

    suspend fun get(market: Market, orderType: OrderType): DelayedTradeProcessor {
        mutex.withLock {
            val key = tuple(market, orderType)
            val processor = processors[key]

            if (processor != null) {
                logger.debug { "Delayed Trade Processor already exists for ($market, $orderType)." }
                return processor
            }

            logger.debug { "Creating new Delayed Trade Processor for ($market, $orderType)..." }

            val orderBook = poloniexApi.getOrderBookFlowBy(market)
            val newProcessor = DelayedTradeProcessor(
                market,
                orderType,
                orderBook,
                scope + CoroutineName("DELAYED_TRADE_PROCESSOR_${market}_$orderType"),
                splitAlgo,
                poloniexApi,
                amountCalculator,
                clock
            )
            processors[key] = newProcessor

            val processorJob = newProcessor.start()

            scope.launch(Job() + CoroutineName("DELAYED_TRADE_MANAGER_${market}_$orderType"), CoroutineStart.UNDISPATCHED) {
                withContext(NonCancellable) {
                    processorJob.join()
                    logger.debug { "Delayed Trade Processor has completed its job in ($market, $orderType) market." }

                    mutex.withLock {
                        processors.remove(key)
                    }

                    logger.debug { "Delayed Trade Processor for ($market, $orderType) market has been removed from processors list." }
                }
            }

            return newProcessor
        }
    }

    companion object {
        private val logger = KotlinLogging.logger {}
    }
}
