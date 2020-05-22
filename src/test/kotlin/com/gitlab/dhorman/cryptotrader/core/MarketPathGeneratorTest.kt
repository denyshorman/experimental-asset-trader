package com.gitlab.dhorman.cryptotrader.core

import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import io.vavr.kotlin.tuple
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

class MarketPathGeneratorTest {
    private val allMarkets = listOf(
        Market("BTC", "ARDR"),
        Market("BTC", "ATOM"),
        Market("BTC", "AVA"),
        Market("BTC", "BAT"),
        Market("BTC", "BCH"),
        Market("BTC", "BCHABC"),
        Market("BTC", "BCHSV"),
        Market("BTC", "BNT"),
        Market("BTC", "BTS"),
        Market("BTC", "BURST"),
        Market("BTC", "CHR"),
        Market("BTC", "CVC"),
        Market("BTC", "DASH"),
        Market("BTC", "DCR"),
        Market("BTC", "DOGE"),
        Market("BTC", "EOS"),
        Market("BTC", "ETC"),
        Market("BTC", "ETH"),
        Market("BTC", "ETHBNT"),
        Market("BTC", "FOAM"),
        Market("BTC", "FXC"),
        Market("BTC", "GAS"),
        Market("BTC", "GNT"),
        Market("BTC", "GRIN"),
        Market("BTC", "HUC"),
        Market("BTC", "KNC"),
        Market("BTC", "LINK"),
        Market("BTC", "LOOM"),
        Market("BTC", "LPT"),
        Market("BTC", "LSK"),
        Market("BTC", "LTC"),
        Market("BTC", "MANA"),
        Market("BTC", "MATIC"),
        Market("BTC", "MKR"),
        Market("BTC", "NEO"),
        Market("BTC", "NMC"),
        Market("BTC", "NMR"),
        Market("BTC", "NXT"),
        Market("BTC", "OMG"),
        Market("BTC", "POLY"),
        Market("BTC", "PPC"),
        Market("BTC", "QTUM"),
        Market("BTC", "REP"),
        Market("BTC", "SBD"),
        Market("BTC", "SC"),
        Market("BTC", "SNT"),
        Market("BTC", "SNX"),
        Market("BTC", "STEEM"),
        Market("BTC", "STORJ"),
        Market("BTC", "STR"),
        Market("BTC", "STRAT"),
        Market("BTC", "SWFTC"),
        Market("BTC", "SYS"),
        Market("BTC", "TRX"),
        Market("BTC", "XCP"),
        Market("BTC", "XEM"),
        Market("BTC", "XMR"),
        Market("BTC", "XRP"),
        Market("BTC", "XTZ"),
        Market("BTC", "ZEC"),
        Market("BTC", "ZRX"),
        Market("DAI", "BTC"),
        Market("DAI", "ETH"),
        Market("ETH", "BAT"),
        Market("ETH", "BCH"),
        Market("ETH", "EOS"),
        Market("ETH", "ETC"),
        Market("ETH", "REP"),
        Market("ETH", "ZEC"),
        Market("ETH", "ZRX"),
        Market("PAX", "BTC"),
        Market("PAX", "ETH"),
        Market("TRX", "AVA"),
        Market("TRX", "BTT"),
        Market("TRX", "CHR"),
        Market("TRX", "ETH"),
        Market("TRX", "FXC"),
        Market("TRX", "JST"),
        Market("TRX", "LINK"),
        Market("TRX", "MATIC"),
        Market("TRX", "NEO"),
        Market("TRX", "SNX"),
        Market("TRX", "STEEM"),
        Market("TRX", "SWFTC"),
        Market("TRX", "WIN"),
        Market("TRX", "XRP"),
        Market("TRX", "XTZ"),
        Market("USDC", "ATOM"),
        Market("USDC", "BCH"),
        Market("USDC", "BCHABC"),
        Market("USDC", "BCHSV"),
        Market("USDC", "BTC"),
        Market("USDC", "DASH"),
        Market("USDC", "DOGE"),
        Market("USDC", "EOS"),
        Market("USDC", "ETC"),
        Market("USDC", "ETH"),
        Market("USDC", "GRIN"),
        Market("USDC", "LTC"),
        Market("USDC", "STR"),
        Market("USDC", "TRX"),
        Market("USDC", "USDT"),
        Market("USDC", "XMR"),
        Market("USDC", "XRP"),
        Market("USDC", "ZEC"),
        Market("USDJ", "BTC"),
        Market("USDJ", "BTT"),
        Market("USDJ", "TRX"),
        Market("USDT", "ATOM"),
        Market("USDT", "AVA"),
        Market("USDT", "BAT"),
        Market("USDT", "BCH"),
        Market("USDT", "BCHABC"),
        Market("USDT", "BCHBEAR"),
        Market("USDT", "BCHSV"),
        Market("USDT", "BCHBULL"),
        Market("USDT", "BCN"),
        Market("USDT", "BEAR"),
        Market("USDT", "BSVBEAR"),
        Market("USDT", "BSVBULL"),
        Market("USDT", "BTC"),
        Market("USDT", "BTT"),
        Market("USDT", "BULL"),
        Market("USDT", "BVOL"),
        Market("USDT", "CHR"),
        Market("USDT", "DAI"),
        Market("USDT", "DASH"),
        Market("USDT", "DGB"),
        Market("USDT", "DOGE"),
        Market("USDT", "EOS"),
        Market("USDT", "EOSBEAR"),
        Market("USDT", "EOSBULL"),
        Market("USDT", "ETC"),
        Market("USDT", "ETH"),
        Market("USDT", "ETHBEAR"),
        Market("USDT", "ETHBULL"),
        Market("USDT", "FXC"),
        Market("USDT", "GNT"),
        Market("USDT", "GRIN"),
        Market("USDT", "IBVOL"),
        Market("USDT", "JST"),
        Market("USDT", "LINK"),
        Market("USDT", "LINKBEAR"),
        Market("USDT", "LINKBULL"),
        Market("USDT", "LSK"),
        Market("USDT", "LTC"),
        Market("USDT", "MANA"),
        Market("USDT", "MATIC"),
        Market("USDT", "MKR"),
        Market("USDT", "NEO"),
        Market("USDT", "NXT"),
        Market("USDT", "PAX"),
        Market("USDT", "QTUM"),
        Market("USDT", "REP"),
        Market("USDT", "SC"),
        Market("USDT", "SNX"),
        Market("USDT", "STEEM"),
        Market("USDT", "STR"),
        Market("USDT", "SWFTC"),
        Market("USDT", "TRX"),
        Market("USDT", "TRXBEAR"),
        Market("USDT", "TRXBULL"),
        Market("USDT", "USDJ"),
        Market("USDT", "WIN"),
        Market("USDT", "XMR"),
        Market("USDT", "XRP"),
        Market("USDT", "XRPBEAR"),
        Market("USDT", "XRPBULL"),
        Market("USDT", "XTZ"),
        Market("USDT", "ZEC"),
        Market("USDT", "ZRX")
    )

    @Test
    fun `Generate paths for USDT USDC and add to file`() {
        Files.newBufferedWriter(Path.of("build/reports/USDT_USDC_paths.txt")).use { writer ->
            MarketPathGenerator(allMarkets).generate(tuple("USDT", "USDC")).forEach { path ->
                val str = path.joinToString(" | ") { it.toString() }
                writer.appendln(str)
            }
        }
    }

    @Test
    fun `Generate paths for USDT USDC`() {
        MarketPathGenerator(allMarkets).generate(tuple("USDT", "USDT")).forEach { path ->
            val str = path.joinToString(" | ") { it.toString() }
            println(str)
        }
    }

    @Test
    fun `Generate path permutations for USDT,USDC,USDJ,PAX,DAI`() {
        val primaryCurrencies = listOf("USDT", "USDC", "USDJ", "PAX", "DAI")
        Files.newBufferedWriter(Path.of("build/reports/USDT_USDC_USDJ_PAX_DAI_paths.txt")).use { writer ->
            MarketPathGenerator(allMarkets).generateAll(primaryCurrencies).forEach { entry ->
                entry.value.forEach { path ->
                    val str = path.joinToString(" | ") { it.toString() }
                    writer.appendln(str)
                }
            }
        }
    }

    @Test
    fun `Generate path permutations with orders for USDT,USDC,USDJ,PAX,DAI`() {
        val primaryCurrencies = listOf("USDT", "USDC", "USDJ", "PAX", "DAI")
        Files.newBufferedWriter(Path.of("build/reports/USDT_USDC_USDJ_PAX_DAI_path_with_orders.txt")).use { writer ->
            MarketPathGenerator(allMarkets).generateAllPermutationsWithOrders(primaryCurrencies).forEach { entry ->
                entry.value.forEach { path ->
                    val str = path.joinToString(" | ") { (s, m) ->
                        "$m${if (s == OrderSpeed.Instant) "0" else "1"}"
                    }
                    writer.appendln(str)
                }
            }
        }
    }
}
