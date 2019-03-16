package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.service.poloniex.PoloniexApi
import mu.KotlinLogging
import reactor.core.publisher.Flux
import reactor.core.publisher.ReplayProcessor

class RawStreams(
    private val trigger: TriggerStreams,
    private val poloniexApi: PoloniexApi
) {
    private val logger = KotlinLogging.logger {}

    private fun <T> wrap(triggerStream: ReplayProcessor<Unit>, stream: Flux<T>): Flux<T> {
        return triggerStream.startWith(Unit).switchMap({ stream.take(1) }, 1).replay(1).refCount()
    }

    val currencies = run {
        val callApiStream = poloniexApi.currencies().flux().doOnNext { curr ->
            logger.info("${curr.length()} currencies fetched")
        }
        wrap(trigger.currencies, callApiStream)
    }

    val ticker = run {
        val callApiStream = poloniexApi.ticker().flux().doOnNext { tickers ->
            logger.info("All tickers fetched")

            if(logger.isDebugEnabled) logger.debug(tickers.toString())
        }
        wrap(trigger.ticker, callApiStream)
    }

    val balances = run {
        val callApiStream = poloniexApi.balances().flux().doOnNext { balances ->
            logger.info("All available balances fetched")
            if(logger.isDebugEnabled) logger.debug(balances.toString())
        }
        wrap(trigger.balances, callApiStream)
    }

    val openOrders = run {
        val callApiStream = poloniexApi.allOpenOrders().flux().doOnNext { orders ->
            logger.info("All open orders fetched")

            if(logger.isDebugEnabled) logger.debug(orders.toString())
        }
        wrap(trigger.openOrders, callApiStream)
    }

    val accountNotifications = poloniexApi.accountNotificationStream

    val tickerStream = poloniexApi.tickerStream
}