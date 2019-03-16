package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.TradeStatOrder
import com.gitlab.dhorman.cryptotrader.core.cut8
import io.vavr.collection.Queue
import io.vavr.collection.Array
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.MathContext
import java.math.RoundingMode
import java.time.Instant
import java.util.*

class TradeStatTest {
    private val random = Random()

    private fun calcTrueStat(lastTrades: Queue<TradeStatModels.SimpleTrade>): TradeStatOrder {

        val waitMsSample = if (lastTrades.length() == 1) {
            Array.of(lastTrades.head().timestamp.toEpochMilli())
        } else {
            lastTrades.iterator()
                .map { it.timestamp.toEpochMilli() }
                .sliding(2)
                .map { t -> t.head() - t.last() }
                .toArray()
        }

        val waitMsSampleSize = waitMsSample.size().toBigDecimal()

        val avg = waitMsSample.reduce { a, b -> a + b }.toBigDecimal().setScale(12, RoundingMode.HALF_EVEN) / waitMsSampleSize

        val variance = run {
            val squareSum = waitMsSample.map { v ->
                val d = v.toBigDecimal() - avg
                d * d
            }.reduce { a, b -> a + b }

            squareSum.setScale(12, RoundingMode.HALF_EVEN) / waitMsSampleSize
        }

        val stdDev = variance.sqrt(MathContext.DECIMAL128).setScale(0, RoundingMode.DOWN)

        val sizeAmount = lastTrades.iterator().map { it.amount }.size().toBigDecimal()
        val minAmount = lastTrades.iterator().map { it.amount }.min()
        val maxAmount = lastTrades.iterator().map { it.amount }.max()
        val avgAmount = run {
            val am = lastTrades.iterator().map { it.amount }
            if (sizeAmount.compareTo(BigDecimal.ZERO) == 0) BigDecimal(0) else am.reduce { a, b -> a + b }.setScale(12, RoundingMode.HALF_EVEN) / sizeAmount
        }
        val varianceAmount = lastTrades.iterator()
            .map { it.amount }
            .map { a -> run { val x = a - avgAmount; x * x } }
            .reduce { a, b -> a + b }.setScale(12, RoundingMode.HALF_EVEN) / sizeAmount
        val stdDevAmount = varianceAmount.sqrt(MathContext.DECIMAL128).setScale(8, RoundingMode.DOWN)

        return TradeStatOrder(
            avg.toLong(),
            variance.toLong(),
            stdDev.toLong(),
            minAmount.getOrElse(Double.MAX_VALUE.toBigDecimal()),
            maxAmount.getOrElse(Double.MIN_VALUE.toBigDecimal()),
            avgAmount,
            varianceAmount,
            stdDevAmount,
            lastTrades.last().timestamp,
            lastTrades.head().timestamp
        )
    }

    private fun assert(stat: TradeStatOrder, res: TradeStatModels.Trade2State, checkMinMax: Boolean = true) {
        assert(stat.ttwAvgMs == res.ttwAverageMs.toLong())
        assert(stat.ttwVariance == res.ttwVariance.toLong())
        assert(stat.ttwStdDev == res.ttwStdDev.toLong())
        if (checkMinMax) assert(stat.minAmount == res.minAmount)
        if (checkMinMax) assert(stat.maxAmount == res.maxAmount)
        assert(stat.avgAmount.cut8 == res.avgAmount.cut8)
        assert(stat.varianceAmount.cut8 == res.varianceAmount.cut8)
        assert(stat.stdDevAmount.cut8 == res.stdDevAmount.cut8)
        assert(stat.firstTranTs == res.firstTranTs)
        assert(stat.lastTranTs == res.lastTranTs)
    }

    @Test
    fun `Trade2State calcFull should correctly calculate stat with random trades`() {
        val randomTime = Math.abs(random.nextLong()) % 500 + 1

        val sample = Queue.of(
            TradeStatModels.SimpleTrade(
                3.toBigDecimal(),
                (Math.abs(random.nextLong()) % 500 + 1).toBigDecimal(),
                Instant.ofEpochMilli(randomTime)
            ),
            TradeStatModels.SimpleTrade(
                2.toBigDecimal(),
                (Math.abs(random.nextLong()) % 500 + 1).toBigDecimal(),
                Instant.ofEpochMilli(randomTime - 10)
            ),
            TradeStatModels.SimpleTrade(
                1.toBigDecimal(),
                (Math.abs(random.nextLong()) % 500 + 1).toBigDecimal(),
                Instant.ofEpochMilli(randomTime - 34)
            )
        )

        val expected: TradeStatOrder = calcTrueStat(sample)
        val actual: TradeStatModels.Trade2State = TradeStatModels.Trade2State.calcFull(sample)

        println(sample)
        println(actual)

        assert(expected, actual)
    }

    @Test
    fun `Trade2State calcFull should correctly calculate stat with specific trades`() {
        val sample = Queue.of(
            TradeStatModels.SimpleTrade(3.toBigDecimal(), 429.toBigDecimal(), Instant.ofEpochMilli(461)),
            TradeStatModels.SimpleTrade(2.toBigDecimal(), 402.toBigDecimal(), Instant.ofEpochMilli(451)),
            TradeStatModels.SimpleTrade(1.toBigDecimal(), 308.toBigDecimal(), Instant.ofEpochMilli(427))
        )

        val expected: TradeStatOrder = calcTrueStat(sample)
        val actual: TradeStatModels.Trade2State = TradeStatModels.Trade2State.calcFull(sample)

        println(sample)
        println(actual)

        assert(expected, actual)
    }

    @Test
    fun `Trade2State calc should correctly calculate stat with random trades`() {
        val randomTime = Math.abs(random.nextLong()) % 500 + 1

        val sample0 = Queue.of(
            TradeStatModels.SimpleTrade(
                3.toBigDecimal(),
                (Math.abs(random.nextLong()) % 500 + 1).toBigDecimal(),
                Instant.ofEpochMilli(randomTime - 5)
            ),
            TradeStatModels.SimpleTrade(
                2.toBigDecimal(),
                (Math.abs(random.nextLong()) % 500 + 1).toBigDecimal(),
                Instant.ofEpochMilli(randomTime - 8)
            ),
            TradeStatModels.SimpleTrade(
                1.toBigDecimal(),
                (Math.abs(random.nextLong()) % 500 + 1).toBigDecimal(),
                Instant.ofEpochMilli(randomTime - 15)
            )
        )
        val sample1 = Queue.of(
            TradeStatModels.SimpleTrade(4.toBigDecimal(), 121.toBigDecimal(), Instant.ofEpochMilli(randomTime)),
            sample0[0],
            sample0[1],
            sample0[2]
        )

        val initState: TradeStatModels.Trade2State = TradeStatModels.Trade2State.calcFull(sample0)
        val expected: TradeStatOrder = calcTrueStat(sample1)
        val actual = TradeStatModels.Trade2State.calc(initState, sample0, sample1)

        println(expected)
        println(actual)

        assert(expected, actual)
    }

    @Test
    fun `Trade2State calc should correctly calculate stat with specific trades`() {
        val sample0 = Queue.of(
            TradeStatModels.SimpleTrade(3.toBigDecimal(), 429.toBigDecimal(), Instant.ofEpochMilli(11)),
            TradeStatModels.SimpleTrade(2.toBigDecimal(), 402.toBigDecimal(), Instant.ofEpochMilli(8)),
            TradeStatModels.SimpleTrade(1.toBigDecimal(), 308.toBigDecimal(), Instant.ofEpochMilli(1))
        )
        val sample1 = Queue.of(
            TradeStatModels.SimpleTrade(3.toBigDecimal(), 121.toBigDecimal(), Instant.ofEpochMilli(18)),
            sample0[0],
            sample0[1],
            sample0[2]
        )

        val initState: TradeStatModels.Trade2State = TradeStatModels.Trade2State.calcFull(sample0)
        val expected: TradeStatOrder = calcTrueStat(sample1)
        val actual = TradeStatModels.Trade2State.calc(initState, sample0, sample1)

        println(expected)
        println(actual)

        assert(expected, actual)
    }

    @Test
    fun `Trade2State calc should correctly calculate stat with specific trades 2`() {
        val sample0 = Queue.of(
            TradeStatModels.SimpleTrade(3.toBigDecimal(), 3.toBigDecimal(), Instant.ofEpochMilli(11)),
            TradeStatModels.SimpleTrade(2.toBigDecimal(), 1.toBigDecimal(), Instant.ofEpochMilli(8)),
            TradeStatModels.SimpleTrade(1.toBigDecimal(), 2.toBigDecimal(), Instant.ofEpochMilli(1))
        )

        val sample1 = Queue.of(
            TradeStatModels.SimpleTrade(4.toBigDecimal(), 4.toBigDecimal(), Instant.ofEpochMilli(18)),
            sample0[0],
            sample0[1],
            sample0[2]
        )

        val sample2 = Queue.of(
            TradeStatModels.SimpleTrade(5.toBigDecimal(), 5.toBigDecimal(), Instant.ofEpochMilli(38)),
            sample1[0],
            sample1[1],
            sample1[2]
        )

        val state0 = TradeStatModels.Trade2State.calcFull(sample0)
        val state1 = TradeStatModels.Trade2State.calc(state0, sample0, sample1)
        val actual = TradeStatModels.Trade2State.calc(state1, sample1, sample2)
        val expected = calcTrueStat(sample2)

        println(expected)
        println(actual)

        assert(expected, actual)
    }

    @Test
    fun `Trade2State calc should correctly calculate stat with specific trades 3`() {
        var ts = 1L
        var prevSample: Queue<TradeStatModels.SimpleTrade>?
        var sample = Queue.empty<TradeStatModels.SimpleTrade>()
        var state: TradeStatModels.Trade2State = TradeStatModels.Trade2State.calcFull(sample)
        val maxSample = 15

        for (i in 1..100) {
            val price = (Math.abs(random.nextLong()) % 100 + 1).toBigDecimal()
            val amount = (Math.abs(random.nextLong()) % 500 + 1).toBigDecimal()
            ts += i
            val trade = TradeStatModels.SimpleTrade(price, amount, Instant.ofEpochMilli(ts))

            prevSample = sample

            sample = if (sample.length() == maxSample) {
                sample.dropRight(1).prepend(trade)
            } else {
                sample.prepend(trade)
            }

            state = TradeStatModels.Trade2State.calc(state, prevSample, sample)
        }

        val expected = calcTrueStat(sample)
        val actual = state

        println(expected)
        println(actual)

        assert(expected, actual, checkMinMax = false)
    }
}
