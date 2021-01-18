package com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.core

import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.OrderType

class OrderBookEmptyException(bookType: SubBookType) : Throwable("Order book $bookType is empty") {
    constructor(orderType: OrderType) : this(if (orderType == OrderType.Buy) SubBookType.Buy else SubBookType.Sell)
}

enum class SubBookType { Buy, Sell }
