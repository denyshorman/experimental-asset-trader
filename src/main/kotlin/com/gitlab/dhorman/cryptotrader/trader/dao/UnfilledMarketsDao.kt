package com.gitlab.dhorman.cryptotrader.trader.dao

import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import io.vavr.Tuple2
import org.springframework.data.r2dbc.function.DatabaseClient
import org.springframework.stereotype.Repository

@Repository
class UnfilledMarketsDao(private val databaseClient: DatabaseClient) {
    suspend fun getAll(initFromCurrency: Currency, fromCurrency: Currency): List<Tuple2<Amount, Amount>>? {
        TODO()
    }

    suspend fun removeAll(initFromCurrency: Currency, fromCurrency: Currency) {
        TODO()
    }

    suspend fun add(
        initCurrency: Currency,
        initCurrencyAmount: Amount,
        currentCurrency: Currency,
        currentCurrencyAmount: Amount
    ): Long {
        TODO()
    }
}