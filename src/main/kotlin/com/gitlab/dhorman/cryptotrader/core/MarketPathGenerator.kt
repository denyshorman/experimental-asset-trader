package com.gitlab.dhorman.cryptotrader.core

import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import io.vavr.Tuple2
import io.vavr.collection.*
import io.vavr.collection.Array
import io.vavr.collection.Iterator
import io.vavr.collection.List
import io.vavr.collection.List.range
import io.vavr.collection.Map
import io.vavr.collection.Set
import io.vavr.kotlin.*
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant

class MarketPathGenerator(availableMarkets: Traversable<Market>) {

    private val allPaths: Map<Currency, Set<Tuple2<Tuple2<Currency, Currency>, Market>>> = availableMarkets
        .iterator()
        .flatMap { m -> hashSet(tuple(tuple(m.b, m.q), m), tuple(tuple(m.q, m.b), m)) }
        .groupBy { it._1._1 }
        .mapValues { it.toSet() }

    /**
     * Generate paths only for a->b
     */
    fun generate(targetPath: TargetPath): Set<Path> {
        return f(targetPath).filter { it.nonEmpty() }
    }

    /**
     * Generate all path permutations (a->a) (a->b) (b->a) (b->b)
     */
    fun generateAll(targetPath: TargetPath): Map<TargetPath, Set<Path>> {
        val (a, b) = targetPath
        val p = Array.of(tuple(a, a), tuple(a, b), tuple(b, a), tuple(b, b))

        return hashMap(
            p[0] to generate(p[0]),
            p[1] to generate(p[1]),
            p[2] to generate(p[2]),
            p[3] to generate(p[3])
        )
    }

    fun generateAllPermutationsWithOrders(currencies: Traversable<Currency>): Map<TargetPath, Set<List<Tuple2<PathOrderType, Market>>>> {
        val permutations = currencies.flatMap { a ->
            currencies.map { b ->
                val target = tuple(a, b)
                val paths = generate(target).flatMap { generateAllPermutationsWithOrders(it) }
                tuple(target, paths)
            }
        }

        return permutations.toMap({ it._1 }, { it._2 })
    }

    private fun generateAllPermutationsWithOrders(path: List<Market>): Seq<List<Tuple2<PathOrderType, Market>>> {
        return range(0, (1 shl path.length())).map { i ->
            path.zipWithIndex().map { (market, j) ->
                if ((i and (1 shl j)) == 0) {
                    tuple(PathOrderType.Delayed, market)
                } else {
                    tuple(PathOrderType.Instant, market)
                }
            }
        }
    }

    private fun f1(targetPath: TargetPath): Set<Path> {
        return allPaths.get(targetPath._1).get()
            .iterator()
            .find { it._1._2 == targetPath._2 }
            .map { hashSet(List.of(it._2)) }
            .getOrElse { hashSet(List.empty()) }
    }

    private fun f2(targetPath: TargetPath): Iterator<Path> {
        // (a->x - y->b)
        val (p, q) = targetPath

        return allPaths.get(p).get().iterator().filter { it._1._2 != q }.flatMap { l0 ->
            val (x0, m1) = l0
            val (_, x) = x0

            allPaths.get(x).get().iterator().filter { it._1._2 == q }.map { (_, m2) ->
                list(m1, m2)
            }
        }
    }

    private fun f3(targetPath: TargetPath): Iterator<Path> {
        // (a->x - y->z - k->b)
        val (p, q) = targetPath

        return allPaths.get(p).get().iterator().filter { it._1._2 != q }.flatMap { (l0, m1) ->
            val (_, x) = l0

            allPaths.get(x).get().iterator()
                .filter { h -> h._1._2 != p && h._1._2 != q }
                .flatMap { (l1, m2) ->
                    val (_, z) = l1
                    allPaths.get(z).get().iterator().filter { it._1._2 == q }.map { (_, m3) ->
                        list(m1, m2, m3)
                    }
                }
        }
    }

    private fun f4(targetPath: TargetPath): Iterator<Path> {
        // (a->x - y->z - i->j - k->b)
        val (p, q) = targetPath

        return allPaths.get(p).get().iterator()
            .filter { it._1._2 != q }
            .flatMap { (l0, m1) ->
                val (_, x) = l0

                allPaths.get(x).get().iterator()
                    .filter { h -> h._1._2 != p && h._1._2 != q }
                    .flatMap { (l1, m2) ->
                        val (y, z) = l1
                        allPaths.get(z).get().iterator()
                            .filter { h -> h._1._2 != p && h._1._2 != q && h._1._2 != y }
                            .flatMap { (l2, m3) ->
                                val (_, j) = l2

                                allPaths.get(j).get().iterator()
                                    .filter { it._1._2 == q }
                                    .map { (_, m4) ->
                                        list(m1, m2, m3, m4)
                                    }
                            }
                    }
            }
    }

    private fun f(targetPath: TargetPath): Set<Path> {
        return f1(targetPath)
            .addAll(f2(targetPath))
            .addAll(f3(targetPath))
            .addAll(f4(targetPath))
    }
}

typealias TargetPath = Tuple2<Currency, Currency>
typealias Path = List<Market>

data class TargetPath0(val from: Currency, val to: Currency)
typealias InstantDelayedOrderChain = List<InstantDelayedOrder>

data class ExhaustivePath(
    val targetPath: TargetPath,
    val chain: InstantDelayedOrderChain
) {
    val id: String by lazy {
        chain.iterator().map {
            when (it) {
                is InstantOrder -> "${it.market}0"
                is DelayedOrder -> "${it.market}1"
            }
        }.mkString("")
    }

    val simpleMultiplier: BigDecimal by lazy {
        chain.iterator().map {
            when (it) {
                is InstantOrder -> it.orderMultiplierSimple
                is DelayedOrder -> it.orderMultiplier
            }
        }.reduceLeft { a, b -> a * b }
    }

    val amountMultiplier: BigDecimal by lazy {
        chain.iterator().map {
            when (it) {
                is InstantOrder -> it.orderMultiplierAmount
                is DelayedOrder -> it.orderMultiplier
            }
        }.reduceLeft { a, b -> a * b }
    }

    val avgWaitTime: Long by lazy {
        chain.iterator().map {
            when (it) {
                is InstantOrder -> 0
                is DelayedOrder -> it.stat.ttwAvgMs
            }
        }.reduceLeft { a, b -> a + b }
    }

    val maxWaitTime: Long by lazy {
        chain.iterator().map {
            when (it) {
                is InstantOrder -> 0
                is DelayedOrder -> it.stat.ttwAvgMs + it.stat.ttwStdDev
            }
        }.reduceLeft { a, b -> a + b }
    }

    val recommendedStartAmount: Amount by lazy {
        var recommendedStartAmount: BigDecimal? = null
        var targetCurrency = targetPath._2

        for (order in chain.reverseIterator()) {
            when (order) {
                is DelayedOrder ->
                    recommendedStartAmount = if (order.market.quoteCurrency == targetCurrency) {
                        if (recommendedStartAmount == null) {
                            order.stat.avgAmount.setScale(12, RoundingMode.HALF_EVEN) / order.orderMultiplier
                        } else {
                            (order.stat.avgAmount.min(recommendedStartAmount)).setScale(12, RoundingMode.HALF_EVEN) / order.orderMultiplier
                        }
                    } else {
                        if (recommendedStartAmount == null) {
                            order.stat.avgAmount
                        } else {
                            (recommendedStartAmount * order.orderMultiplier).min(order.stat.avgAmount)
                        }
                    }
                else -> run {}
            }

            targetCurrency = when (order) {
                is DelayedOrder -> order.market.other(targetCurrency)!!
                is InstantOrder -> order.market.other(targetCurrency)!!
            }
        }

        recommendedStartAmount ?: when (val head = chain.head()) {
            is DelayedOrder -> head.fromAmount
            is InstantOrder -> head.fromAmount
        }
    }

    val simpleRisk: Int by lazy {
        var bit = 0
        var orderType = 0
        chain.reverseIterator().forEach { order ->
            when (order) {
                is InstantOrder -> run {}
                is DelayedOrder -> orderType = orderType or (1 shl bit)
            }
            bit++
        }

        sr[chain.length() - 1][orderType]
    }

    val lastTran: Long by lazy {
        val now = Instant.now()
        val oldTranTime = chain.iterator().map {
            when (it) {
                is InstantOrder -> now
                is DelayedOrder -> it.stat.lastTranTs
            }
        }.min().get()

        now.toEpochMilli() - oldTranTime.toEpochMilli()
    }

    override fun hashCode(): Int = id.hashCode()

    override fun equals(other: Any?): Boolean = when (other) {
        is ExhaustivePath -> id == other.id
        else -> false
    }

    companion object {
        private val sr1 = arrayOf(0, 0)
        private val sr2 = arrayOf(0, 100, 0, 200)
        private val sr3 = arrayOf(0, 100, 110, 200, 0, 210, 210, 300)
        private val sr4 = arrayOf(0, 100, 110, 200, 120, 210, 210, 300, 0, 240, 240, 310, 220, 310, 310, 400)
        private val sr = arrayOf(sr1, sr2, sr3, sr4)
    }
}


enum class PathOrderType { Instant, Delayed }
