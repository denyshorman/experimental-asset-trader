package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage

import io.vavr.collection.SortedMap
import io.vavr.collection.TreeMap
import java.math.BigDecimal

data class OrderBook(
    val asks: SortedMap<BigDecimal, BigDecimal> = TreeMap.empty(),
    val bids: SortedMap<BigDecimal, BigDecimal> = TreeMap.empty(compareByDescending { it })
)
