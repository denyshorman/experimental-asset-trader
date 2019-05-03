package com.gitlab.dhorman.cryptotrader.core

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

    fun generateWithOrders(targetPath: TargetPath): Set<List<Tuple2<OrderSpeed, Market>>> {
        return generate(targetPath).flatMap { generateAllPermutationsWithOrders(it) }
    }

    fun generateWithOrders(
        fromCurrencies: Traversable<Currency>,
        toCurrencies: Traversable<Currency>
    ): Map<TargetPath, Set<List<Tuple2<OrderSpeed, Market>>>> {
        val permutations = fromCurrencies.flatMap { a ->
            toCurrencies.map { b ->
                val target = tuple(a, b)
                val paths = generateWithOrders(target)
                tuple(target, paths)
            }
        }

        return permutations.toMap({ it._1 }, { it._2 })
    }

    fun generateAllPermutationsWithOrders(currencies: Traversable<Currency>): Map<TargetPath, Set<List<Tuple2<OrderSpeed, Market>>>> {
        val permutations = currencies.flatMap { a ->
            currencies.map { b ->
                val target = tuple(a, b)
                val paths = generateWithOrders(target)
                tuple(target, paths)
            }
        }

        return permutations.toMap({ it._1 }, { it._2 })
    }

    private fun generateAllPermutationsWithOrders(path: List<Market>): Seq<List<Tuple2<OrderSpeed, Market>>> {
        return range(0, (1 shl path.length())).map { i ->
            path.zipWithIndex().map { (market, j) ->
                if ((i and (1 shl j)) == 0) {
                    tuple(OrderSpeed.Delayed, market)
                } else {
                    tuple(OrderSpeed.Instant, market)
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
