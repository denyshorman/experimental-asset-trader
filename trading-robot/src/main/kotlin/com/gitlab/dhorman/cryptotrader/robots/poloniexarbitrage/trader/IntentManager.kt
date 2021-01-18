package com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader

import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.model.TranIntentMarket
import io.vavr.collection.Array
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import mu.KotlinLogging
import org.springframework.stereotype.Component
import java.util.*

@Component
class IntentManager {
    private val paths = LinkedList<TransactionIntent>()
    private val mutex = Mutex()

    suspend fun getAll(): List<TransactionIntent> {
        return mutex.withLock {
            val pathsCopy = LinkedList<TransactionIntent>()
            pathsCopy.addAll(paths)
            pathsCopy
        }
    }

    suspend fun get(markets: Array<TranIntentMarket>, marketIdx: Int): TransactionIntent? {
        return mutex.withLock {
            logger.debug { "Trying to find similar intent in path manager..." }

            val intent = paths.find { it.marketIdx == marketIdx && areEqual(it.markets, markets) }

            if (intent != null) {
                logger.debug { "Intent has been found in path manager" }
            } else {
                logger.debug { "Intent has not been found in path manager" }
            }

            intent
        }
    }

    suspend fun add(intent: TransactionIntent) {
        mutex.withLock {
            paths.add(intent)
            logger.debug { "Intent has been added to path manager" }
        }
    }

    suspend fun remove(intentId: UUID) {
        mutex.withLock {
            paths.removeIf { it.id == intentId }
            logger.debug { "Intent has been removed from path manager" }
        }
    }

    companion object {
        private val logger = KotlinLogging.logger {}

        private fun areEqual(markets0: Array<TranIntentMarket>, markets1: Array<TranIntentMarket>): Boolean {
            if (markets0.length() != markets1.length()) return false
            var i = 0
            while (i < markets0.length()) {
                val equal = markets0[i].market == markets1[i].market
                    && markets0[i].orderSpeed == markets1[i].orderSpeed
                    && markets0[i].fromCurrencyType == markets1[i].fromCurrencyType
                if (!equal) return false
                i++
            }
            return true
        }
    }
}
