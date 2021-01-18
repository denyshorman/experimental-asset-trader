package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.wallet

import kotlinx.coroutines.flow.StateFlow
import java.math.BigDecimal

interface Wallet {
    val balance: StateFlow<BigDecimal>
}
