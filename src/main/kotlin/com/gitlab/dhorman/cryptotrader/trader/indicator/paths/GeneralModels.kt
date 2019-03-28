package com.gitlab.dhorman.cryptotrader.trader.indicator.paths

import com.gitlab.dhorman.cryptotrader.core.ExhaustivePath
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import io.vavr.collection.List
import java.util.Comparator

object ExhaustivePathOrdering : Comparator<ExhaustivePath> {
    override fun compare(x: ExhaustivePath, y: ExhaustivePath): Int {
        return x.id.compareTo(y.id)
    }
}

data class PathsSettings(
    val initialAmount: Amount,
    val currencies: List<Currency>,
    val recalculatePeriodSec: Long = 15
)