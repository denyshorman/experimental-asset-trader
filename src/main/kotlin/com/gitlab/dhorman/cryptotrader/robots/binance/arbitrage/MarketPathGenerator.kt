package com.gitlab.dhorman.cryptotrader.robots.binance.arbitrage

import com.gitlab.dhorman.cryptotrader.robots.binance.arbitrage.model.Currency
import com.gitlab.dhorman.cryptotrader.robots.binance.arbitrage.model.Market
import com.gitlab.dhorman.cryptotrader.robots.binance.arbitrage.model.OrderSpeed

class MarketPathGenerator(availableMarkets: Iterable<Market>) {
    //region Init
    private val allPaths: Map<Currency, Set<Pair<Pair<Currency, Currency>, Market>>> = availableMarkets
        .asSequence()
        .flatMap { m -> sequenceOf(Pair(Pair(m.b, m.q), m), Pair(Pair(m.q, m.b), m)) }
        .groupBy { it.first.first }
        .mapValues { it.value.toSet() }
    //endregion

    //region Generate paths
    /**
     * Generate paths only for a->b
     */
    fun generate(targetPath: Pair<Currency, Currency>): Sequence<List<Market>> {
        return f(targetPath)
    }

    /**
     * Generate all path permutations (a->a) (a->b) (b->a) (b->b)
     */
    fun generateAll(currencies: Iterable<Currency>): Map<Pair<Currency, Currency>, Sequence<List<Market>>> {
        val map = hashMapOf<Pair<Currency, Currency>, Sequence<List<Market>>>()
        for (a in currencies) {
            for (b in currencies) {
                val target = Pair(a, b)
                val paths = generate(target)
                map[target] = paths
            }
        }
        return map
    }
    //endregion

    //region Generate paths with orders
    fun generateWithOrders(targetPath: Pair<Currency, Currency>): Sequence<Sequence<Pair<OrderSpeed, Market>>> {
        return generate(targetPath).flatMap { generateAllPermutationsWithOrders(it) }
    }

    fun generateWithOrders(
        fromCurrencies: Iterable<Currency>,
        toCurrencies: Iterable<Currency>
    ): Map<Pair<Currency, Currency>, Sequence<Sequence<Pair<OrderSpeed, Market>>>> {
        val map = hashMapOf<Pair<Currency, Currency>, Sequence<Sequence<Pair<OrderSpeed, Market>>>>()
        for (a in fromCurrencies) {
            for (b in toCurrencies) {
                val target = Pair(a, b)
                val paths = generateWithOrders(target)
                map[target] = paths
            }
        }
        return map
    }

    fun generateAllPermutationsWithOrders(currencies: Iterable<Currency>): Map<Pair<Currency, Currency>, Sequence<Sequence<Pair<OrderSpeed, Market>>>> {
        return generateWithOrders(currencies, currencies)
    }
    //endregion

    //region Paths utils
    private fun generateAllPermutationsWithOrders(path: List<Market>): Sequence<Sequence<Pair<OrderSpeed, Market>>> {
        return (0..(1 shl path.size)).asSequence().map { i ->
            path.asSequence().mapIndexed { j, market ->
                if ((i and (1 shl j)) == 0) {
                    Pair(OrderSpeed.Delayed, market)
                } else {
                    Pair(OrderSpeed.Instant, market)
                }
            }
        }
    }

    private fun f1(targetPath: Pair<Currency, Currency>): Sequence<List<Market>> {
        val market = allPaths[targetPath.first]?.find { it.first.second == targetPath.second }?.second
        return if (market == null) emptySequence() else sequenceOf(listOf(market))
    }

    private fun f2(targetPath: Pair<Currency, Currency>): Sequence<List<Market>> {
        // (a->x - y->b)
        val (p, q) = targetPath

        return allPaths.getOrDefault(p, defaultPath).asSequence().filter { it.first.second != q }.flatMap { l0 ->
            val (x0, m1) = l0
            val (_, x) = x0

            allPaths.getOrDefault(x, defaultPath).asSequence().filter { it.first.second == q }.map { (_, m2) ->
                listOf(m1, m2)
            }
        }
    }

    private fun f3(targetPath: Pair<Currency, Currency>): Sequence<List<Market>> {
        // (a->x - y->z - k->b)
        val (p, q) = targetPath

        return allPaths.getOrDefault(p, defaultPath).asSequence().filter { it.first.second != q }.flatMap { (l0, m1) ->
            val (_, x) = l0

            allPaths.getOrDefault(x, defaultPath).asSequence()
                .filter { h -> h.first.second != p && h.first.second != q }
                .flatMap { (l1, m2) ->
                    val (_, z) = l1
                    allPaths.getOrDefault(z, defaultPath).asSequence().filter { it.first.second == q }.map { (_, m3) ->
                        listOf(m1, m2, m3)
                    }
                }
        }
    }

    private fun f4(targetPath: Pair<Currency, Currency>): Sequence<List<Market>> {
        // (a->x - y->z - i->j - k->b)
        val (p, q) = targetPath

        return allPaths.getOrDefault(p, defaultPath).asSequence()
            .filter { it.first.second != q }
            .flatMap { (l0, m1) ->
                val (_, x) = l0

                allPaths.getOrDefault(x, defaultPath).asSequence()
                    .filter { h -> h.first.second != p && h.first.second != q }
                    .flatMap { (l1, m2) ->
                        val (y, z) = l1
                        allPaths.getOrDefault(z, defaultPath).asSequence()
                            .filter { h -> h.first.second != p && h.first.second != q && h.first.second != y }
                            .flatMap { (l2, m3) ->
                                val (_, j) = l2

                                allPaths.getOrDefault(j, defaultPath).asSequence()
                                    .filter { it.first.second == q }
                                    .map { (_, m4) ->
                                        listOf(m1, m2, m3, m4)
                                    }
                            }
                    }
            }
    }

    private fun f(targetPath: Pair<Currency, Currency>): Sequence<List<Market>> {
        return f1(targetPath) + f2(targetPath) + f3(targetPath) + f4(targetPath)
    }
    //endregion

    companion object {
        private val defaultPath = emptySet<Pair<Pair<Currency, Currency>, Market>>()
    }
}
