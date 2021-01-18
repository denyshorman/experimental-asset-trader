package com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader

import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.core.AmountType
import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.core.BareTrade
import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.core.fromAmount
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.Amount
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.OrderType
import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.algo.SplitTradeAlgo
import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.core.AdjustedPoloniexBuySellAmountCalculator
import io.vavr.collection.List
import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import io.vavr.kotlin.toVavrList
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import mu.KotlinLogging
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.atomic.AtomicReference

class TradeScheduler(
    private val orderType: OrderType,
    private val splitAlgo: SplitTradeAlgo,
    private val amountCalculator: AdjustedPoloniexBuySellAmountCalculator
) {
    private val ids = LinkedList<PathId>()
    private val idFromAmount = hashMapOf<PathId, Amount>()
    private val idOutput = hashMapOf<PathId, Channel<List<BareTrade>>>()
    private val idStatusNew = hashMapOf<PathId, Boolean>()
    private val mutex = Mutex()

    private val idFromAmountCommon = AtomicReference(BigDecimal.ZERO)

    val fromAmount: Amount
        get() {
            return idFromAmountCommon.get()
        }

    suspend fun register(id: PathId, outputTrades: Channel<List<BareTrade>>) {
        mutex.withLock {
            logger.debug { "Trying to register path in Trade Scheduler..." }

            ids.addLast(id)
            idStatusNew[id] = true
            idFromAmount[id] = BigDecimal.ZERO
            idOutput[id] = outputTrades

            logger.debug { "Path has been successfully registered in Trade Scheduler..." }
        }
    }

    suspend fun unregister(id: PathId) {
        mutex.withLock {
            logger.debug { "Trying to unregister path from Trade Scheduler..." }

            if (!pathExists(id)) {
                logger.debug { "Path was already unregistered from Trade Scheduler earlier." }
                return
            }

            ids.remove(id)
            idStatusNew.remove(id)
            idFromAmount.remove(id)
            idOutput.remove(id)?.close()

            recalculateCommonFromAmount()

            logger.debug { "Path has been successfully removed from Trade Scheduler" }
        }
    }

    suspend fun unregisterAll(error: Throwable? = null) {
        mutex.withLock {
            logger.debug { "Start unregistering all paths..." }
            for (id in ids.toVavrList()) {
                if (idStatusNew[id] == true) continue

                ids.remove(id)
                idStatusNew.remove(id)
                idFromAmount.remove(id)
                idOutput.remove(id)?.close(error)

                logger.debug { "Unregistered path $id" }
            }
            recalculateCommonFromAmount()
            logger.debug { "All paths have been unregistered" }
        }
    }

    fun pathExists(id: PathId): Boolean {
        return ids.contains(id)
    }

    suspend fun addAmount(id: PathId, fromAmount: Amount): Boolean {
        mutex.withLock {
            logger.debug { "Trying to add amount $fromAmount to trade scheduler..." }

            val added = run {
                val output = idOutput[id]
                val idAmount = idFromAmount[id]
                if (output == null || idAmount == null || output.isClosedForSend) return@run false
                idFromAmount[id] = idAmount + fromAmount
                return@run true
            }

            if (added) {
                idStatusNew[id] = false
                recalculateCommonFromAmount()
                logger.debug { "Amount $fromAmount has been added to trade scheduler" }
            } else {
                logger.debug { "Amount $fromAmount has not been added to trade scheduler" }
            }

            return added
        }
    }

    suspend fun addTrades(tradeList: Collection<BareTrade>) {
        if (tradeList.isEmpty()) return

        mutex.withLock {
            logger.debug {
                val clients = ids.joinToString { "($it, ${idFromAmount[it]})" }
                "Trying to split trades $tradeList between clients $clients with commonFromAmount ${idFromAmountCommon.get()}..."
            }

            val receivedTrades = LinkedList(tradeList)
            val clientTrades = mutableMapOf<PathId, MutableList<BareTrade>>()

            while (true) {
                val trade = receivedTrades.pollFirst() ?: break
                val tradeFromAmount = amountCalculator.fromAmount(orderType, trade)
                var tradeConsumed = false

                logger.debug { "Trying to find a client for the trade $trade..." }

                for (clientId in ids) {
                    val clientFromAmount = idFromAmount.getValue(clientId)

                    if (clientFromAmount < BigDecimal.ZERO) {
                        logger.error("clientFromAmount $clientFromAmount is less than zero")
                        continue
                    } else if (clientFromAmount.compareTo(BigDecimal.ZERO) == 0) {
                        continue
                    } else if (tradeFromAmount < BigDecimal.ZERO) {
                        if (clientFromAmount + tradeFromAmount < BigDecimal.ZERO) {
                            logger.debug {
                                "Trade can't be delivered to client $clientId because tradeFromAmount is negative ($tradeFromAmount) " +
                                    "and fromAmount of the client ($clientFromAmount) can't accept this trade: " +
                                    "resulting client's from amount will become less than zero ${clientFromAmount + tradeFromAmount} < 0"
                            }
                            continue
                        } else {
                            logger.debug {
                                "Client $clientId matched received trade. " +
                                    "tradeFromAmount is negative ($tradeFromAmount) but " +
                                    "resulting client's amount ${clientFromAmount + tradeFromAmount} can handle this trade"
                            }
                            idFromAmount[clientId] = clientFromAmount + tradeFromAmount
                            clientTrades.getOrPut(clientId, { mutableListOf() }).add(trade)
                            tradeConsumed = true
                        }
                    } else if (tradeFromAmount > clientFromAmount) {
                        logger.debug { "Trying to split $clientId [tradeFromAmount ($tradeFromAmount) > clientFromAmount ($clientFromAmount)]" }

                        idFromAmount[clientId] = BigDecimal.ZERO

                        val (lTrades, rTrades) = splitAlgo.splitTrade(AmountType.From, orderType, clientFromAmount, trade)

                        logger.debug {
                            "Split trade for $clientId: " +
                                "splitTrade(trade = $trade, orderType = $orderType, fromAmount = $clientFromAmount) => " +
                                "(lTrades = $lTrades, rTrades = $rTrades)"
                        }

                        receivedTrades.addAll(lTrades)
                        clientTrades.getOrPut(clientId, { mutableListOf() }).addAll(rTrades)
                        tradeConsumed = true
                    } else {
                        logger.debug { "Path $clientId matched received trade [tradeFromAmount ($tradeFromAmount) <= clientFromAmount ($clientFromAmount)]" }
                        idFromAmount[clientId] = clientFromAmount - tradeFromAmount
                        clientTrades.getOrPut(clientId, { mutableListOf() }).add(trade)
                        tradeConsumed = true
                    }

                    if (tradeConsumed) break
                }

                if (!tradeConsumed) {
                    logger.error("No clients were found for received trade $trade.")
                }
            }

            clientTrades.forEach { (id, trades) ->
                val output = idOutput.getValue(id)
                val fromAmount = idFromAmount[id]!!

                logger.debug { "Sending trade $trades to $id..." }
                output.send(trades.toVavrList())
                logger.debug { "Path $id has been processed the trade $trades" }

                if (fromAmount.compareTo(BigDecimal.ZERO) == 0) {
                    logger.debug { "Path's amount $id is equal to zero. Closing output channel..." }
                    output.close()
                    logger.debug { "Output channel for $id has been closed" }
                }
            }

            logger.debug { "All trades $tradeList have been processed by a trade scheduler." }

            recalculateCommonFromAmount()
        }
    }

    private fun recalculateCommonFromAmount() {
        val newFromAmount = idFromAmount.asSequence().map { it.value }.fold(BigDecimal.ZERO) { a, b -> a + b }
        idFromAmountCommon.set(newFromAmount)
    }

    companion object {
        private val logger = KotlinLogging.logger {}
    }
}
