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
