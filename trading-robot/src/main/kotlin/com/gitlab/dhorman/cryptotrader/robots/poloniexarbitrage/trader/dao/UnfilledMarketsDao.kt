package com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.dao

import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.Amount
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.Currency
import io.vavr.Tuple2
import io.vavr.collection.List
import io.vavr.kotlin.toVavrList
import io.vavr.kotlin.tuple
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.stereotype.Repository
import java.math.BigDecimal

@Repository
class UnfilledMarketsDao(@Qualifier("pg_client") private val entityTemplate: R2dbcEntityTemplate) {
    suspend fun getAll(): List<UnfilledData> {
        return entityTemplate.databaseClient.sql(
            """
            SELECT init_currency, init_currency_amount, current_currency, current_currency_amount
            FROM poloniex_unfilled_markets
            """.trimIndent()
        )
            .fetch().all()
            .map {
                UnfilledData(
                    it["init_currency"] as Currency,
                    it["init_currency_amount"] as Amount,
                    it["current_currency"] as Currency,
                    it["current_currency_amount"] as Amount
                )
            }
            .collectList()
            .map { it.toVavrList() }
            .awaitSingle()
    }

    suspend fun getAllCurrenciesWithInitAmountMoreOrEqual(threshold: BigDecimal): kotlin.collections.List<UnfilledData> {
        return entityTemplate.databaseClient.sql("SELECT init_currency, init_currency_amount, current_currency, current_currency_amount FROM poloniex_unfilled_markets WHERE init_currency_amount >= $1")
            .bind(0, threshold)
            .fetch().all()
            .map {
                UnfilledData(
                    it["init_currency"] as Currency,
                    it["init_currency_amount"] as Amount,
                    it["current_currency"] as Currency,
                    it["current_currency_amount"] as Amount
                )
            }
            .collectList()
            .awaitSingle()
    }

    suspend fun get(id: Long): UnfilledData? {
        return entityTemplate.databaseClient.sql(
            """
            SELECT init_currency, init_currency_amount, current_currency, current_currency_amount
            FROM poloniex_unfilled_markets
            WHERE id = $1
            """.trimIndent()
        )
            .bind(0, id)
            .fetch().first()
            .map {
                UnfilledData(
                    it["init_currency"] as Currency,
                    it["init_currency_amount"] as Amount,
                    it["current_currency"] as Currency,
                    it["current_currency_amount"] as Amount
                )
            }
            .awaitFirstOrNull()
    }

    suspend fun get(initFromCurrencies: Iterable<Currency>, fromCurrency: Currency): List<Tuple2<Amount, Amount>> {
        val initCurrencies = initFromCurrencies.joinToString(",") { "'$it'" } // TODO: Remove when r2dbc postgres adds support from IN stmt

        return entityTemplate.databaseClient.sql(
            """
            SELECT init_currency_amount, current_currency_amount
            FROM poloniex_unfilled_markets
            WHERE init_currency IN ($initCurrencies) AND current_currency = $1
            """.trimIndent()
        )
            // .bind(0, initFromCurrencies.toJavaArray())
            .bind(0, fromCurrency)
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

    suspend fun remove(initFromCurrencies: Iterable<Currency>, fromCurrency: Currency) {
        val initCurrencies = initFromCurrencies.joinToString(",") { "'$it'" } // TODO: Remove when r2dbc postgres adds support from IN stmt

        entityTemplate.databaseClient.sql("DELETE FROM poloniex_unfilled_markets WHERE init_currency IN ($initCurrencies) AND current_currency = $1")
            .bind(0, fromCurrency)
            .then()
            .awaitFirstOrNull()
    }

    suspend fun remove(id: Long) {
        entityTemplate.databaseClient.sql("DELETE FROM poloniex_unfilled_markets WHERE id = $1")
            .bind(0, id)
            .then()
            .awaitFirstOrNull()
    }

    suspend fun add(
        initCurrency: Currency,
        initCurrencyAmount: Amount,
        currentCurrency: Currency,
        currentCurrencyAmount: Amount
    ) {
        entityTemplate.databaseClient.sql(
            """
            INSERT INTO poloniex_unfilled_markets(init_currency, init_currency_amount, current_currency, current_currency_amount)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (init_currency, current_currency) DO UPDATE SET
            init_currency_amount = EXCLUDED.init_currency_amount + poloniex_unfilled_markets.init_currency_amount,
            current_currency_amount = EXCLUDED.current_currency_amount + poloniex_unfilled_markets.current_currency_amount
            """.trimIndent()
        )
            .bind(0, initCurrency)
            .bind(1, initCurrencyAmount)
            .bind(2, currentCurrency)
            .bind(3, currentCurrencyAmount)
            .then()
            .awaitFirstOrNull()
    }

    suspend fun add(initCurrency: Currency, initCurrencyAmount: Amount, unfilledAmounts: List<Tuple2<Currency, Amount>>) {
        if (unfilledAmounts.size() == 0) return

        val values = unfilledAmounts.asSequence().map { "('$initCurrency','$initCurrencyAmount','${it._1}','${it._2}')" }.joinToString()

        entityTemplate.databaseClient.sql(
            """
            INSERT INTO poloniex_unfilled_markets(init_currency, init_currency_amount, current_currency, current_currency_amount)
            VALUES $values
            ON CONFLICT (init_currency, current_currency) DO UPDATE SET
            init_currency_amount = EXCLUDED.init_currency_amount + poloniex_unfilled_markets.init_currency_amount,
            current_currency_amount = EXCLUDED.current_currency_amount + poloniex_unfilled_markets.current_currency_amount
            """.trimIndent()
        )
            .then()
            .awaitFirstOrNull()
    }
}

data class UnfilledData(
    val initCurrency: Currency,
    val initAmount: Amount,
    val currency: Currency,
    val amount: Amount
)
