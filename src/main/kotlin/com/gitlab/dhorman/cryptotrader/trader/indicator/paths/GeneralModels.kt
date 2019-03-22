package com.gitlab.dhorman.cryptotrader.trader.indicator.paths

import com.gitlab.dhorman.cryptotrader.core.ExhaustivePath
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import io.vavr.collection.List
import java.util.Comparator

object ExhaustivePathOrdering : Comparator<ExhaustivePath> {
    override fun compare(x: ExhaustivePath, y: ExhaustivePath): Int {
        return if (x.id == y.id) {
            0
        } else {
            x.simpleMultiplier.compareTo(y.simpleMultiplier)
        }
    }
}

data class PathsSettings(
    val initialAmount: Amount,
    val currencies: List<Currency>
)