package com.gitlab.dhorman.cryptotrader.robots.binancearbitrage.model

data class SimulatedPath(val orderIntents: List<OrderIntent>) {
    data class OrderIntent(
        val market: Market,
        val orderSpeed: OrderSpeed
    )
}

fun SimulatedPath.toShortString(): String {
    return orderIntents.asSequence().map {
        when (it.orderSpeed) {
            OrderSpeed.Instant -> "${it.market}0"
            OrderSpeed.Delayed -> "${it.market}1"
        }
    }.joinToString("->")
}

fun SimulatedPath.targetCurrency(fromCurrency: Currency): Currency? {
    var targetCurrency = fromCurrency
    orderIntents.forEach { orderIntent ->
        targetCurrency = orderIntent.market.other(targetCurrency) ?: return null
    }
    return targetCurrency
}

fun SimulatedPath.marketSpeedCount(orderSpeed: OrderSpeed): Int {
    var counter = 0
    for (orderIntent in orderIntents) {
        if (orderIntent.orderSpeed == orderSpeed) {
            counter++
        }
    }
    return counter
}

fun SimulatedPath.isRiskFree(): Boolean {
    var i = 0
    while (i < orderIntents.size) {
        if (i > 0 && orderIntents[i].orderSpeed == OrderSpeed.Delayed) {
            return false
        }
        i++
    }
    return true
}
