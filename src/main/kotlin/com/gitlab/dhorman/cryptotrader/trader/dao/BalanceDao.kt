package com.gitlab.dhorman.cryptotrader.trader.dao

import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import io.vavr.collection.Map
import io.vavr.kotlin.toVavrMap
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.data.r2dbc.function.DatabaseClient
import org.springframework.stereotype.Repository
import java.math.BigDecimal

@Repository
class BalanceDao(private val databaseClient: DatabaseClient) {
    suspend fun getAll(): Map<Currency, BigDecimal> {
        return databaseClient.execute().sql("select * from poloniex_balances")
            .fetch()
            .all()
            .collectMap(
                { it["currency"] as String },
                { it["balance"] as BigDecimal }
            )
            .map { it.toVavrMap() }
            .awaitSingle()
    }

    suspend fun removeAll() {
        databaseClient.execute().sql("delete from poloniex_balances").then().awaitFirstOrNull()
    }

    suspend fun get(currency: Currency): BigDecimal? {
        return databaseClient.execute().sql("select balance from poloniex_balances where currency = $1")
            .bind(0, currency)
            .fetch()
            .first()
            .map { it["balance"] as BigDecimal }
            .awaitFirstOrNull()
    }

    suspend fun upsert(currency: Currency, balance: BigDecimal) {
        databaseClient.execute().sql(
            """
            INSERT INTO poloniex_balances(currency, balance) VALUES ($1, $2)
            ON CONFLICT (currency)
            DO UPDATE SET balance = $2
        """.trimIndent()
        )
            .bind(0, currency)
            .bind(1, balance)
            .then()
            .awaitFirstOrNull()
    }

    suspend fun delete(currency: Currency) {
        databaseClient.execute().sql("delete from poloniex_balances where currency = $1")
            .bind(0, currency)
            .then()
            .awaitFirstOrNull()
    }
}