package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.core.TradeStat
import com.gitlab.dhorman.cryptotrader.core.TradeStatOrder
import com.gitlab.dhorman.cryptotrader.service.poloniex.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Price
import io.vavr.Tuple2
import io.vavr.collection.Queue
import io.vavr.collection.Seq
import io.vavr.collection.TreeMap
import mu.KotlinLogging
import reactor.core.publisher.Flux
import java.math.BigDecimal
import java.math.MathContext
import java.math.RoundingMode
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

class Trader(private val poloniexApi: PoloniexApi) {
    private val logger = KotlinLogging.logger {}

    private val trigger = TriggerStreams()
    private val raw = RawStreams(trigger, poloniexApi)
    val data = DataStreams(raw, trigger, poloniexApi)
    val indicators = IndicatorStreams(data)

    fun start(): Flux<Unit> {
        logger.info("Start trading")

        data.tradesStat.switchMap({ map ->
            val allTradeStats: Seq<Flux<TradeStat>> = map.values()

            val i = AtomicLong(0)

            val stat: Flux<Tuple2<Long, TradeStat>> = Flux.empty<Flux<TradeStat>>()
                .startWith(allTradeStats)
                .flatMap({ stat: Flux<TradeStat> ->
                    val id = i.getAndIncrement()
                    stat.map { s -> Tuple2(id, s) }.onBackpressureLatest()
                }, allTradeStats.size(), 1)

            stat
        }, 1)
            .scan(TreeMap.empty<Long, TradeStat>()) { state: TreeMap<Long, TradeStat>, delta: Tuple2<Long, TradeStat> ->
                state.put(delta._1, delta._2)
            }
            .subscribe({ stat ->
                logger.info("Trades fetched: ${stat.size()}")
            }, {
                logger.error(it.message, it)
            })

        return Flux.just(Unit)
    }

    companion object {
        class OpenOrder(
            val id: Long,
            val tpe: OrderType,
            val market: Market,
            val rate: Price,
            val amount: Amount
        ) {
            constructor(id2: Long) : this(id2, OrderType.Sell, Market("BTC", "ETH"), BigDecimal.ZERO, BigDecimal.ZERO)

            override fun hashCode(): Int = id.hashCode()

            override fun equals(other: Any?): Boolean = when (other) {
                is OpenOrder -> other.id == this.id
                else -> false
            }

            override fun toString() = "OpenOrder($id, $tpe, $market, $rate, $amount)"
        }
    }
}

object TradeStatModels {
    data class SimpleTrade(
        val price: Price,
        val amount: Amount,
        val timestamp: Instant
    )

    data class Trade0(
        val sell: Queue<SimpleTrade>,
        val buy: Queue<SimpleTrade>
    )

    data class Trade1(
        val sellOld: Queue<SimpleTrade>,
        val sellNew: Queue<SimpleTrade>,
        val buyOld: Queue<SimpleTrade>,
        val buyNew: Queue<SimpleTrade>,
        val sellStatus: Trade1Status,
        val buyStatus: Trade1Status
    ) {
        companion object Trade1 {
            fun newTrades(newTrade: SimpleTrade, trades: Queue<SimpleTrade>, limit: Int): Queue<SimpleTrade> {
                return if (trades.size() == limit) {
                    trades.dropRight(1).prepend(newTrade)
                } else {
                    trades.prepend(newTrade)
                }
            }
        }
    }

    enum class Trade1Status { Init, Changed, NotChanged }

    data class Trade2(
        val sell: Trade2State,
        val buy: Trade2State
    ) {
        companion object {
            val DEFAULT = Trade2(Trade2State.from0(), Trade2State.from0())
        }
    }

    data class Trade2State(
        val ttwSumMs: BigDecimal,
        val ttwVarianceSum: BigDecimal,
        val sumAmount: BigDecimal,
        val varianceSumAmount: BigDecimal,

        val ttwAverageMs: BigDecimal,
        val ttwVariance: BigDecimal,
        val ttwStdDev: BigDecimal,
        val minAmount: BigDecimal,
        val maxAmount: BigDecimal,
        val avgAmount: BigDecimal,
        val varianceAmount: BigDecimal,
        val stdDevAmount: BigDecimal,
        val firstTranTs: Instant,
        val lastTranTs: Instant
    ) {
        companion object {
            private const val tsNull: Long = -1

            private fun subSquare(a: BigDecimal, b: BigDecimal): BigDecimal {
                val x = a - b
                return x * x
            }

            fun map(stat: Trade2State): TradeStatOrder {
                return TradeStatOrder(
                    stat.ttwAverageMs.toLong(),
                    stat.ttwVariance.toLong(),
                    stat.ttwStdDev.toLong(),
                    stat.minAmount,
                    stat.maxAmount,
                    stat.avgAmount,
                    stat.varianceAmount,
                    stat.stdDevAmount,
                    stat.firstTranTs,
                    stat.lastTranTs
                )
            }

            fun from0(): Trade2State {
                return Trade2State(
                    ttwSumMs = BigDecimal.ZERO,
                    ttwVarianceSum = BigDecimal.ZERO,
                    sumAmount = BigDecimal.ZERO,
                    varianceSumAmount = BigDecimal.ZERO,

                    ttwAverageMs = Double.MAX_VALUE.toBigDecimal(),
                    ttwVariance = Double.MAX_VALUE.toBigDecimal(),
                    ttwStdDev = Double.MAX_VALUE.toBigDecimal(),
                    minAmount = Double.MAX_VALUE.toBigDecimal(),
                    maxAmount = Double.MIN_VALUE.toBigDecimal(),
                    avgAmount = BigDecimal.ZERO,
                    varianceAmount = Double.MAX_VALUE.toBigDecimal(),
                    stdDevAmount = Double.MAX_VALUE.toBigDecimal(),
                    firstTranTs = Instant.EPOCH,
                    lastTranTs = Instant.EPOCH
                )
            }

            fun from1(trade: SimpleTrade): Trade2State {
                return Trade2State(
                    ttwSumMs = BigDecimal.ZERO,
                    ttwVarianceSum = BigDecimal.ZERO,
                    sumAmount = trade.amount,
                    varianceSumAmount = BigDecimal.ZERO,

                    ttwAverageMs = trade.timestamp.toEpochMilli().toBigDecimal(),
                    ttwVariance = BigDecimal.ZERO,
                    ttwStdDev = BigDecimal.ZERO,
                    minAmount = trade.amount,
                    maxAmount = trade.amount,
                    avgAmount = trade.amount,
                    varianceAmount = BigDecimal.ZERO,
                    stdDevAmount = BigDecimal.ZERO,
                    firstTranTs = trade.timestamp,
                    lastTranTs = trade.timestamp
                )
            }

            fun calc(state: Trade2State, old: Queue<SimpleTrade>, new: Queue<SimpleTrade>): Trade2State {
                return if (new.length() == 1) {
                    from1(new.head())
                } else {
                    val newTrade = new.head()
                    val oldTrade = old.last()

                    val ttwSumMs: BigDecimal
                    val ttwVarianceSum: BigDecimal
                    val sumAmount: BigDecimal
                    val varianceSumAmount: BigDecimal

                    val ttwAverageMs: BigDecimal
                    val ttwVariance: BigDecimal
                    val ttwStdDev: BigDecimal
                    val minAmount = state.minAmount.min(newTrade.amount)
                    val maxAmount = state.maxAmount.max(newTrade.amount)
                    val avgAmount: BigDecimal
                    val varianceAmount: BigDecimal
                    val stdDevAmount: BigDecimal
                    val firstTranTs: Instant = new.last().timestamp
                    val lastTranTs: Instant = newTrade.timestamp

                    if (new.length() == old.length()) {
                        sumAmount = state.sumAmount + newTrade.amount - oldTrade.amount
                        varianceSumAmount = run {
                            val oldAvgAmount =
                                state.sumAmount.setScale(12, RoundingMode.HALF_EVEN) / old.length().toBigDecimal()
                            val deltaAmountAvg = (newTrade.amount - oldTrade.amount).setScale(
                                12,
                                RoundingMode.HALF_EVEN
                            ) / old.length().toBigDecimal()
                            val deltaVarianceSumAmount = run {
                                val newAmountValue = subSquare(newTrade.amount, oldAvgAmount)
                                val oldAmountValue = subSquare(oldTrade.amount, oldAvgAmount)
                                newAmountValue - oldAmountValue + deltaAmountAvg * (old.length().toBigDecimal() * (deltaAmountAvg + 2.toBigDecimal() * oldAvgAmount) - 2.toBigDecimal() * sumAmount)
                            }
                            state.varianceSumAmount + deltaVarianceSumAmount
                        }

                        val newTtwValue = newTrade.timestamp.toEpochMilli() - new.get(1).timestamp.toEpochMilli()
                        val oldTtwValue =
                            old.get(old.length() - 2).timestamp.toEpochMilli() - oldTrade.timestamp.toEpochMilli()

                        ttwSumMs = state.ttwSumMs + (newTtwValue - oldTtwValue).toBigDecimal()
                        ttwVarianceSum = run {
                            val size = old.length() - 1
                            val oldAvgTtw = state.ttwSumMs.setScale(12, RoundingMode.HALF_EVEN) / size.toBigDecimal()
                            val deltaAvgTtw = (newTtwValue - oldTtwValue).toBigDecimal().setScale(
                                12,
                                RoundingMode.HALF_EVEN
                            ) / size.toBigDecimal()
                            val deltaVarianceSumTtw = run {
                                val newTtwValue0 = subSquare(newTtwValue.toBigDecimal(), oldAvgTtw)
                                val oldTtwValue0 = subSquare(oldTtwValue.toBigDecimal(), oldAvgTtw)
                                newTtwValue0 - oldTtwValue0 + deltaAvgTtw * (size.toBigDecimal() * (deltaAvgTtw + 2.toBigDecimal() * oldAvgTtw) - 2.toBigDecimal() * ttwSumMs)
                            }
                            state.ttwVarianceSum + deltaVarianceSumTtw
                        }
                    } else {
                        sumAmount = state.sumAmount + newTrade.amount
                        varianceSumAmount = run {
                            val oldAvgAmount =
                                state.sumAmount.setScale(12, RoundingMode.HALF_EVEN) / old.length().toBigDecimal()
                            val deltaAmountAvg =
                                (old.length().toBigDecimal() * newTrade.amount - state.sumAmount).setScale(
                                    12,
                                    RoundingMode.HALF_EVEN
                                ) / (old.length() * new.length()).toBigDecimal()
                            val deltaVarianceSumAmount = run {
                                val newAmountValue = subSquare(newTrade.amount, oldAvgAmount)
                                newAmountValue + deltaAmountAvg * (new.length().toBigDecimal() * (deltaAmountAvg + 2.toBigDecimal() * oldAvgAmount) - 2.toBigDecimal() * sumAmount)
                            }
                            state.varianceSumAmount + deltaVarianceSumAmount
                        }

                        val newTtwValue = newTrade.timestamp.toEpochMilli() - new.get(1).timestamp.toEpochMilli()
                        ttwSumMs = state.ttwSumMs + newTtwValue.toBigDecimal()
                        ttwVarianceSum = run {
                            if (old.length() == 1) {
                                BigDecimal.ZERO
                            } else {
                                val oldSize = old.length() - 1
                                val newSize = old.length()
                                val oldAvgTtw =
                                    state.ttwSumMs.setScale(12, RoundingMode.HALF_EVEN) / oldSize.toBigDecimal()
                                val deltaAvgTtw =
                                    (oldSize.toBigDecimal() * newTtwValue.toBigDecimal() - state.ttwSumMs).setScale(
                                        12,
                                        RoundingMode.HALF_EVEN
                                    ) / (oldSize * newSize).toBigDecimal()
                                val deltaVarianceSumTtw = run {
                                    val newTtwValue0 = subSquare(newTtwValue.toBigDecimal(), oldAvgTtw)
                                    newTtwValue0 + deltaAvgTtw * (newSize.toBigDecimal() * (deltaAvgTtw + 2.toBigDecimal() * oldAvgTtw) - 2.toBigDecimal() * ttwSumMs)
                                }
                                state.ttwVarianceSum + deltaVarianceSumTtw
                            }
                        }
                    }

                    val size = new.length()
                    ttwAverageMs = ttwSumMs.setScale(12, RoundingMode.HALF_EVEN) / (size - 1).toBigDecimal()
                    ttwVariance = ttwVarianceSum.setScale(12, RoundingMode.HALF_EVEN) / (size - 1).toBigDecimal()
                    ttwStdDev = ttwVariance.sqrt(MathContext.DECIMAL128).setScale(0, RoundingMode.DOWN)
                    avgAmount = sumAmount.setScale(12, RoundingMode.HALF_EVEN) / size.toBigDecimal()
                    varianceAmount = varianceSumAmount.setScale(12, RoundingMode.HALF_EVEN) / size.toBigDecimal()
                    stdDevAmount = varianceAmount.sqrt(MathContext.DECIMAL128).setScale(8, RoundingMode.DOWN)

                    Trade2State(
                        ttwSumMs,
                        ttwVarianceSum,
                        sumAmount,
                        varianceSumAmount,

                        ttwAverageMs,
                        ttwVariance,
                        ttwStdDev,
                        minAmount,
                        maxAmount,
                        avgAmount,
                        varianceAmount,
                        stdDevAmount,
                        firstTranTs,
                        lastTranTs
                    )
                }
            }

            fun calcFull(trades: Queue<SimpleTrade>): Trade2State {
                return when {
                    trades.isEmpty -> from0()
                    trades.length() == 1 -> from1(trades.head())
                    else -> {
                        val tradesSize = trades.length()

                        var ttwSumMs: BigDecimal = BigDecimal.ZERO
                        var ttwVarianceSum: BigDecimal = BigDecimal.ZERO
                        var sumAmount: BigDecimal = BigDecimal.ZERO
                        var varianceSumAmount: BigDecimal = BigDecimal.ZERO

                        val ttwAverageMs: BigDecimal
                        val ttwVariance: BigDecimal
                        val ttwStdDev: BigDecimal
                        var minAmount: BigDecimal = Double.MAX_VALUE.toBigDecimal()
                        var maxAmount: BigDecimal = Double.MIN_VALUE.toBigDecimal()
                        val avgAmount: BigDecimal
                        val varianceAmount: BigDecimal
                        val stdDevAmount: BigDecimal
                        val firstTranTs: Instant = trades.last().timestamp
                        val lastTranTs: Instant = trades.head().timestamp

                        var a: Long = tsNull
                        for (t in trades) {
                            if (t.amount < minAmount) minAmount = t.amount
                            if (t.amount > maxAmount) maxAmount = t.amount
                            sumAmount += t.amount

                            if (a == tsNull) {
                                a = t.timestamp.toEpochMilli()
                            } else {
                                val b = t.timestamp.toEpochMilli()
                                ttwSumMs += (a - b).toBigDecimal()
                                a = b
                            }
                        }

                        a = tsNull

                        ttwAverageMs = ttwSumMs.setScale(12, RoundingMode.HALF_EVEN) / (tradesSize - 1).toBigDecimal()
                        avgAmount = sumAmount.setScale(12, RoundingMode.HALF_EVEN) / tradesSize.toBigDecimal()

                        for (t in trades) {
                            varianceSumAmount += subSquare(t.amount, avgAmount)

                            if (a == tsNull) {
                                a = t.timestamp.toEpochMilli()
                            } else {
                                val b = t.timestamp.toEpochMilli()
                                ttwVarianceSum += subSquare((a - b).toBigDecimal(), ttwAverageMs)
                                a = b
                            }
                        }

                        ttwVariance =
                            ttwVarianceSum.setScale(12, RoundingMode.HALF_EVEN) / (tradesSize - 1).toBigDecimal()
                        ttwStdDev = ttwVariance.sqrt(MathContext.DECIMAL128).setScale(0, RoundingMode.DOWN)
                        varianceAmount =
                            varianceSumAmount.setScale(12, RoundingMode.HALF_EVEN) / tradesSize.toBigDecimal()
                        stdDevAmount = varianceAmount.sqrt(MathContext.DECIMAL128).setScale(8, RoundingMode.DOWN)

                        return Trade2State(
                            ttwSumMs,
                            ttwVarianceSum,
                            sumAmount,
                            varianceSumAmount,

                            ttwAverageMs,
                            ttwVariance,
                            ttwStdDev,
                            minAmount,
                            maxAmount,
                            avgAmount,
                            varianceAmount,
                            stdDevAmount,
                            firstTranTs,
                            lastTranTs
                        )
                    }
                }
            }
        }
    }
}