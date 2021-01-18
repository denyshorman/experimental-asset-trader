package com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.core

import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.Currency
import io.vavr.Tuple2
import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import io.vavr.kotlin.tuple

class MarketPathGenerator(availableMarkets: Iterable<Market>) {
    private val allPaths: Map<Currency, Set<Tuple2<Tuple2<Currency, Currency>, Market>>> = availableMarkets
        .asSequence()
        .flatMap { m -> sequenceOf(tuple(tuple(m.b, m.q), m), tuple(tuple(m.q, m.b), m)) }
        .groupBy { it._1._1 }
        .mapValues { it.value.toSet() }

    /**
     * Generate paths only for a->b
     */
    fun generate(targetPath: TargetPath): Sequence<Path> {
        return f(targetPath)
    }

    /**
     * Generate all path permutations (a->a) (a->b) (b->a) (b->b)
     */
    fun generateAll(currencies: Iterable<Currency>): Map<TargetPath, Sequence<Path>> {
        val map = hashMapOf<TargetPath, Sequence<Path>>()
        for (a in currencies) {
            for (b in currencies) {
                val target = tuple(a, b)
                val paths = generate(target)
                map[target] = paths
            }
        }
        return map
    }

    fun generateWithOrders(targetPath: TargetPath): Sequence<Sequence<Tuple2<OrderSpeed, Market>>> {
        return generate(targetPath).flatMap { generateAllPermutationsWithOrders(it) }
    }

    fun generateWithOrders(
        fromCurrencies: Iterable<Currency>,
        toCurrencies: Iterable<Currency>
    ): Map<TargetPath, Sequence<Sequence<Tuple2<OrderSpeed, Market>>>> {
        val map = hashMapOf<TargetPath, Sequence<Sequence<Tuple2<OrderSpeed, Market>>>>()
        for (a in fromCurrencies) {
            for (b in toCurrencies) {
                val target = tuple(a, b)
                val paths = generateWithOrders(target)
                map[target] = paths
            }
        }
        return map
    }

    fun generateAllPermutationsWithOrders(currencies: Iterable<Currency>): Map<TargetPath, Sequence<Sequence<Tuple2<OrderSpeed, Market>>>> {
        return generateWithOrders(currencies, currencies)
    }

    private fun generateAllPermutationsWithOrders(path: List<Market>): Sequence<Sequence<Tuple2<OrderSpeed, Market>>> {
        return (0..(1 shl path.size)).asSequence().map { i ->
            path.asSequence().mapIndexed { j, market ->
                if ((i and (1 shl j)) == 0) {
                    tuple(OrderSpeed.Delayed, market)
                } else {
                    tuple(OrderSpeed.Instant, market)
                }
            }
        }
    }

    private fun f1(targetPath: TargetPath): Sequence<Path> {
        val market = allPaths[targetPath._1]?.find { it._1._2 == targetPath._2 }?._2
        return if (market == null) emptySequence() else sequenceOf(listOf(market))
    }

    private fun f2(targetPath: TargetPath): Sequence<Path> {
        // (a->x - y->b)
        val (p, q) = targetPath

        return allPaths.getOrDefault(p, defaultPath).asSequence().filter { it._1._2 != q }.flatMap { l0 ->
            val (x0, m1) = l0
            val (_, x) = x0

            allPaths.getOrDefault(x, defaultPath).asSequence().filter { it._1._2 == q }.map { (_, m2) ->
                listOf(m1, m2)
            }
        }
    }

    private fun f3(targetPath: TargetPath): Sequence<Path> {
        // (a->x - y->z - k->b)
        val (p, q) = targetPath

        return allPaths.getOrDefault(p, defaultPath).asSequence().filter { it._1._2 != q }.flatMap { (l0, m1) ->
            val (_, x) = l0

            allPaths.getOrDefault(x, defaultPath).asSequence()
                .filter { h -> h._1._2 != p && h._1._2 != q }
                .flatMap { (l1, m2) ->
                    val (_, z) = l1
                    allPaths.getOrDefault(z, defaultPath).asSequence().filter { it._1._2 == q }.map { (_, m3) ->
                        listOf(m1, m2, m3)
                    }
                }
        }
    }

    private fun f4(targetPath: TargetPath): Sequence<Path> {
        // (a->x - y->z - i->j - k->b)
        val (p, q) = targetPath

        return allPaths.getOrDefault(p, defaultPath).asSequence()
            .filter { it._1._2 != q }
            .flatMap { (l0, m1) ->
                val (_, x) = l0

                allPaths.getOrDefault(x, defaultPath).asSequence()
                    .filter { h -> h._1._2 != p && h._1._2 != q }
                    .flatMap { (l1, m2) ->
                        val (y, z) = l1
                        allPaths.getOrDefault(z, defaultPath).asSequence()
                            .filter { h -> h._1._2 != p && h._1._2 != q && h._1._2 != y }
                            .flatMap { (l2, m3) ->
                                val (_, j) = l2

                                allPaths.getOrDefault(j, defaultPath).asSequence()
                                    .filter { it._1._2 == q }
                                    .map { (_, m4) ->
                                        listOf(m1, m2, m3, m4)
                                    }
                            }
                    }
            }
    }

    private fun f(targetPath: TargetPath): Sequence<Path> {
        return f1(targetPath) + f2(targetPath) + f3(targetPath) + f4(targetPath)
    }

    companion object {
        private val defaultPath = emptySet<Tuple2<Tuple2<Currency, Currency>, Market>>()
    }
}

typealias TargetPath = Tuple2<Currency, Currency>
typealias Path = List<Market>
