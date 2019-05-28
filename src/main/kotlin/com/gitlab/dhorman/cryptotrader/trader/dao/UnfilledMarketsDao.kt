package com.gitlab.dhorman.cryptotrader.trader.dao

import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import io.vavr.Tuple2
import io.vavr.collection.List
import io.vavr.kotlin.toVavrList
import io.vavr.kotlin.tuple
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Repository
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Mono
import java.math.BigDecimal

@Repository
class UnfilledMarketsDao(@Qualifier("pg_client") private val databaseClient: DatabaseClient) {
    suspend fun get(initFromCurrency: Currency, fromCurrency: Currency): List<Tuple2<Amount, Amount>> {
        return databaseClient.execute(
            """
            SELECT init_currency_amount, current_currency_amount
            FROM poloniex_unfilled_markets
            WHERE init_currency = $1 AND current_currency = $2
            """.trimIndent()
        )
            .bind(0, initFromCurrency)
            .bind(1, fromCurrency)
            .fetch().all()
            .map {
                tuple(
                    it["init_currency_amount"] as BigDecimal,
                    it["current_currency_amount"] as BigDecimal
                )
            }
            .collectList()
            .map { it.toVavrList() }
            .awaitSingle()
    }

    suspend fun remove(initFromCurrency: Currency, fromCurrency: Currency) {
        databaseClient.execute("DELETE FROM poloniex_unfilled_markets WHERE init_currency = $1 AND current_currency = $2")
            .bind(0, initFromCurrency)
            .bind(1, fromCurrency)
            .then()
            .awaitFirstOrNull()
    }

    suspend fun add(
        initCurrency: Currency,
        initCurrencyAmount: Amount,
        currentCurrency: Currency,
        currentCurrencyAmount: Amount
    ): Long {
        return databaseClient.execute(
            """
            INSERT INTO poloniex_unfilled_markets(init_currency, init_currency_amount, current_currency, current_currency_amount)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            """.trimIndent()
        )
            .bind(0, initCurrency)
            .bind(1, initCurrencyAmount)
            .bind(2, currentCurrency)
            .bind(3, currentCurrencyAmount)
            .fetch().one()
            .map { it["id"] as Long }
            .awaitSingle()
    }
}