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

        indicators.paths.subscribe({
            //logger.info(it.length().toString())
        }, {
            logger.error(it.message, it)
        })

        /*data.tradesStat.switchMap({ map ->
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
            })*/

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
