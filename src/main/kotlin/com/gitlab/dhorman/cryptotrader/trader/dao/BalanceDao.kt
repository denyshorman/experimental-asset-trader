package com.gitlab.dhorman.cryptotrader.trader.dao

import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import io.vavr.collection.Map
import org.springframework.data.r2dbc.function.DatabaseClient
import org.springframework.stereotype.Repository
import java.math.BigDecimal

@Repository
class BalanceDao(private val databaseClient: DatabaseClient) {
    suspend fun getAll(): Map<Currency, BigDecimal> {
        TODO()
    }

    suspend fun removeAll() {
        TODO()
    }

    suspend fun get(currency: Currency): BigDecimal {
        TODO()
    }

    suspend fun upsert(currency: Currency, balance: BigDecimal) {
        TODO()
    }

    suspend fun delete(currency: Currency) {
        TODO()
    }
}