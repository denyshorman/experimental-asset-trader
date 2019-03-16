package com.gitlab.dhorman.cryptotrader.trader

import reactor.core.publisher.ReplayProcessor

class TriggerStreams {
    val currencies: ReplayProcessor<Unit> = ReplayProcessor.cacheLast<Unit>()
    val ticker: ReplayProcessor<Unit> = ReplayProcessor.cacheLast<Unit>()
    val balances: ReplayProcessor<Unit> = ReplayProcessor.cacheLast<Unit>()
    val openOrders: ReplayProcessor<Unit> = ReplayProcessor.cacheLast<Unit>()
}