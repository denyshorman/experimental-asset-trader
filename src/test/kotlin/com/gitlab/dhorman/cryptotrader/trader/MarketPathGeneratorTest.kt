package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import io.vavr.collection.TreeSet
import io.vavr.kotlin.hashSet
import io.vavr.kotlin.list
import io.vavr.kotlin.tuple
import org.junit.jupiter.api.Test
import java.time.Instant

class MarketPathGeneratorTest {

    private val markets = run {
        hashSet(
            Market("BTC", "BCN"),
            Market("BTC", "BTS"),
            Market("BTC", "BURST"),
            Market("BTC", "CLAM"),
            Market("BTC", "DGB"),
            Market("BTC", "DOGE"),
            Market("BTC", "DASH"),
            Market("BTC", "GAME"),
            Market("BTC", "HUC"),
            Market("BTC", "LTC"),
            Market("BTC", "MAID"),
            Market("BTC", "OMNI"),
            Market("BTC", "NAV"),
            Market("BTC", "NMC"),
            Market("BTC", "NXT"),
            Market("BTC", "PPC"),
            Market("BTC", "STR"),
            Market("BTC", "SYS"),
            Market("BTC", "VIA"),
            Market("BTC", "VTC"),
            Market("BTC", "XCP"),
            Market("BTC", "XMR"),
            Market("BTC", "XPM"),
            Market("BTC", "XRP"),
            Market("BTC", "XEM"),
            Market("BTC", "ETH"),
            Market("BTC", "SC"),
            Market("BTC", "FCT"),
            Market("BTC", "DCR"),
            Market("BTC", "LSK"),
            Market("BTC", "LBC"),
            Market("BTC", "STEEM"),
            Market("BTC", "SBD"),
            Market("BTC", "ETC"),
            Market("BTC", "REP"),
            Market("BTC", "ARDR"),
            Market("BTC", "ZEC"),
            Market("BTC", "STRAT"),
            Market("BTC", "PASC"),
            Market("BTC", "GNT"),
            Market("BTC", "BCH"),
            Market("BTC", "ZRX"),
            Market("BTC", "CVC"),
            Market("BTC", "OMG"),
            Market("BTC", "GAS"),
            Market("BTC", "STORJ"),
            Market("BTC", "EOS"),
            Market("BTC", "SNT"),
            Market("BTC", "KNC"),
            Market("BTC", "BAT"),
            Market("BTC", "LOOM"),
            Market("BTC", "QTUM"),
            Market("BTC", "BNT"),
            Market("BTC", "MANA"),
            Market("BTC", "FOAM"),
            Market("BTC", "BCHABC"),
            Market("BTC", "BCHSV"),
            Market("BTC", "NMR"),
            Market("BTC", "POLY"),
            Market("BTC", "LPT"),
            Market("USDT", "BTC"),
            Market("USDT", "DOGE"),
            Market("USDT", "DASH"),
            Market("USDT", "LTC"),
            Market("USDT", "NXT"),
            Market("USDT", "STR"),
            Market("USDT", "XMR"),
            Market("USDT", "XRP"),
            Market("USDT", "ETH"),
            Market("USDT", "SC"),
            Market("USDT", "LSK"),
            Market("USDT", "ETC"),
            Market("USDT", "REP"),
            Market("USDT", "ZEC"),
            Market("USDT", "GNT"),
            Market("USDT", "BCH"),
            Market("USDT", "ZRX"),
            Market("USDT", "EOS"),
            Market("USDT", "SNT"),
            Market("USDT", "KNC"),
            Market("USDT", "BAT"),
            Market("USDT", "LOOM"),
            Market("USDT", "QTUM"),
            Market("USDT", "BNT"),
            Market("USDT", "MANA"),
            Market("XMR", "BCN"),
            Market("XMR", "DASH"),
            Market("XMR", "LTC"),
            Market("XMR", "MAID"),
            Market("XMR", "NXT"),
            Market("XMR", "ZEC"),
            Market("ETH", "LSK"),
            Market("ETH", "STEEM"),
            Market("ETH", "ETC"),
            Market("ETH", "REP"),
            Market("ETH", "ZEC"),
            Market("ETH", "GNT"),
            Market("ETH", "BCH"),
            Market("ETH", "ZRX"),
            Market("ETH", "CVC"),
            Market("ETH", "OMG"),
            Market("ETH", "GAS"),
            Market("ETH", "EOS"),
            Market("ETH", "SNT"),
            Market("ETH", "KNC"),
            Market("ETH", "BAT"),
            Market("ETH", "LOOM"),
            Market("ETH", "QTUM"),
            Market("ETH", "BNT"),
            Market("ETH", "MANA"),
            Market("USDC", "BTC"),
            Market("USDC", "DOGE"),
            Market("USDC", "LTC"),
            Market("USDC", "STR"),
            Market("USDC", "USDT"),
            Market("USDC", "XMR"),
            Market("USDC", "XRP"),
            Market("USDC", "ETH"),
            Market("USDC", "ZEC"),
            Market("USDC", "BCH"),
            Market("USDC", "FOAM"),
            Market("USDC", "BCHABC"),
            Market("USDC", "BCHSV"),
            Market("USDC", "GRIN")
        )
    }

    private val currencies = markets.flatMap { m -> setOf(m.b, m.q) }

    private val marketPathGenerator = MarketPathGenerator(markets)

    @Test
    fun `MarketPathGenerator should generate for USDC - USDT`() {
        val paths = marketPathGenerator.generate(tuple("USDC", "USDT"))
        paths.toList().sortBy { it.size() }.forEach { p ->
            println(p)
        }
    }

    @Test
    fun `MarketPathGenerator should generate all possible ways between currency A - A`() {
        val set = currencies.flatMap { c -> marketPathGenerator.generate(tuple(c, c)) }
        println(set.size())
    }

    @Test
    fun `MarketPathGenerator should generate all permutations for USDC and USDT`() {
        val paths = marketPathGenerator.generateAll(tuple("USDC", "USDT"))
        val flattenPaths = paths.flatMap { p -> p._2 }.toList().sortBy { it.size() }
        for (path in flattenPaths) println(path)
    }

    @Test
    fun `MarketPathGenerator should generate all permutations with orders`() {
        val paths = marketPathGenerator.generateAllPermutationsWithOrders(list("USDC", "USDT"))
        val flattenPaths = paths.flatMap { p -> p._2 }.toList().sortBy { it.size() }
        // Files.write(Paths.get("./test.txt"), flattenPaths.map { it.toString() })
        println("Total: ${flattenPaths.size()}")
    }

    @Test
    fun `MarketPathGenerator ExhaustivePath should generate json`() {
        val i = InstantOrder(
            Market("USDC", "BTC"),
            "BTC",
            "BTC",
            40.toBigDecimal(),
            0.5.toBigDecimal(),
            OrderType.Buy,
            1.2.toBigDecimal(),
            1.3.toBigDecimal(),
            0.toBigDecimal(),
            0.99.toBigDecimal(),
            list()
        )
        val d = DelayedOrder(
            Market("USDT", "BTC"),
            "BTC",
            "USDT",
            0.5.toBigDecimal(),
            1.2.toBigDecimal(),
            40.toBigDecimal(),
            0.998.toBigDecimal(),
            41.toBigDecimal(),
            OrderType.Sell,
            1.2.toBigDecimal(),
            TradeStatOrder(
                0,
                0,
                0,
                0.toBigDecimal(),
                0.toBigDecimal(),
                0.toBigDecimal(),
                0.toBigDecimal(),
                0.toBigDecimal(),
                Instant.MIN,
                Instant.MIN
            )
        )

        val json = ExhaustivePath(tuple("USDC", "USDT"), list(i, d))

        println(json)
    }

    @Test
    fun `MarketPathGenerator ExhaustivePath TreeMap should generate json`() {
        val i = InstantOrder(
            Market("USDC", "BTC"),
            "BTC",
            "BTC",
            40.toBigDecimal(),
            0.5.toBigDecimal(),
            OrderType.Buy,
            1.2.toBigDecimal(),
            1.3.toBigDecimal(),
            0.toBigDecimal(),
            0.99.toBigDecimal(),
            list()
        )
        val d = DelayedOrder(
            Market("USDT", "BTC"),
            "BTC",
            "USDT",
            0.5.toBigDecimal(),
            1.2.toBigDecimal(),
            40.toBigDecimal(),
            0.998.toBigDecimal(),
            41.toBigDecimal(),
            OrderType.Sell,
            1.2.toBigDecimal(),
            TradeStatOrder(
                0,
                0,
                0,
                0.toBigDecimal(),
                0.toBigDecimal(),
                0.toBigDecimal(),
                0.toBigDecimal(),
                0.toBigDecimal(),
                Instant.MIN,
                Instant.MIN
            )
        )

        val path = ExhaustivePath(tuple("USDC", "USDT"), list(i, d))

        var state = TreeSet.empty<ExhaustivePath> { x, y ->
            x.simpleMultiplier.compareTo(y.simpleMultiplier)
        }

        state = state.add(path)
        state = state.add(path)

        assert(state.size() == 1)
    }
}
