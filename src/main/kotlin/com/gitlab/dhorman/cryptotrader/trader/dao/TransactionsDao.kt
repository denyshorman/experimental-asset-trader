package com.gitlab.dhorman.cryptotrader.trader.dao

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import com.fasterxml.jackson.module.kotlin.readValue
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.trader.TranIntentMarket
import com.gitlab.dhorman.cryptotrader.trader.TranIntentMarketPartiallyCompleted
import com.gitlab.dhorman.cryptotrader.trader.Views
import io.vavr.Tuple2
import io.vavr.Tuple4
import io.vavr.collection.Array
import io.vavr.collection.Traversable
import io.vavr.kotlin.toVavrStream
import io.vavr.kotlin.tuple
import kotlinx.coroutines.reactive.awaitFirstOrDefault
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Repository
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDateTime
import java.util.*

@Repository
class TransactionsDao(
    @Qualifier("pg_client") private val databaseClient: DatabaseClient,
    private val mapper: ObjectMapper
) {
    suspend fun getAll(): List<Tuple2<UUID, Array<TranIntentMarket>>> {
        return databaseClient.execute()
            .sql("SELECT id, markets FROM poloniex_active_transactions")
            .fetch().all()
            .map {
                tuple(
                    it["id"] as UUID,
                    mapper.readValue<Array<TranIntentMarket>>(it["markets"] as String)
                )
            }
            .collectList()
            .awaitSingle()
    }

    suspend fun add(id: UUID, markets: Array<TranIntentMarket>, activeMarketId: Int) {
        val marketsJson = mapper
            .writerFor(jacksonTypeRef<Array<TranIntentMarket>>())
            .withView(Views.DB::class.java)
            .writeValueAsString(markets)

        val fromCurrency = markets[activeMarketId].fromCurrency
        val fromAmount = (markets[activeMarketId] as TranIntentMarketPartiallyCompleted).fromAmount

        databaseClient.execute()
            .sql("INSERT INTO poloniex_active_transactions(id, markets, from_currency, from_amount) VALUES ($1, $2, $3, $4)")
            .bind(0, id)
            .bind(1, marketsJson)
            .bind(2, fromCurrency)
            .bind(3, fromAmount)
            .then()
            .awaitFirstOrNull()
    }

    suspend fun delete(id: UUID) {
        databaseClient.execute()
            .sql("DELETE FROM poloniex_active_transactions WHERE id = $1")
            .bind(0, id)
            .then()
            .awaitFirstOrNull()
    }

    suspend fun update(id: UUID, markets: Array<TranIntentMarket>, activeMarketId: Int) {
        val marketsJson = mapper
            .writerFor(jacksonTypeRef<Array<TranIntentMarket>>())
            .withView(Views.DB::class.java)
            .writeValueAsString(markets)

        val fromCurrency = markets[activeMarketId].fromCurrency
        val fromAmount = (markets[activeMarketId] as TranIntentMarketPartiallyCompleted).fromAmount

        databaseClient.execute()
            .sql("UPDATE poloniex_active_transactions SET markets = $1, from_currency = $2, from_amount = $3 WHERE id = $4")
            .bind(0, marketsJson)
            .bind(1, fromCurrency)
            .bind(2, fromAmount)
            .bind(3, id)
            .then()
            .awaitFirstOrNull()
    }

    suspend fun getOrderIds(tranId: UUID): List<Long> {
        return databaseClient.execute()
            .sql("SELECT order_id FROM poloniex_transaction_order_ids WHERE tran_id = $1 ORDER BY order_id DESC LIMIT 32")
            .bind(0, tranId)
            .fetch().all()
            .map { it["order_id"] as Long }
            .collectList()
            .awaitFirstOrDefault(emptyList())
    }

    suspend fun getTimestampLatestOrderId(tranId: UUID): Instant? {
        return databaseClient.execute()
            .sql("SELECT added_ts FROM poloniex_transaction_order_ids WHERE tran_id = $1 ORDER BY order_id DESC LIMIT 1")
            .bind(0, tranId)
            .fetch().one()
            .map { it["added_ts"] as Instant }
            .awaitFirstOrNull()
    }

    // TODO: Implement deletion of old records
    suspend fun addOrderId(tranId: UUID, orderId: Long) {
        databaseClient.execute()
            .sql("INSERT INTO poloniex_transaction_order_ids(tran_id, order_id) VALUES($1, $2)")
            .bind(0, tranId)
            .bind(1, orderId)
            .then()
            .awaitFirstOrNull()
    }

    suspend fun removeOrderIds(tranId: UUID, orderIds: Traversable<Long>) {
        val orderIdsStr = orderIds.joinToString()
        databaseClient.execute()
            .sql("DELETE FROM poloniex_transaction_order_ids WHERE tran_id = $1 AND order_id IN ($orderIdsStr)")
            .bind(0, tranId)
            .then()
            .awaitFirstOrNull()
    }

    suspend fun getCompleted(): List<Tuple4<Long, Array<TranIntentMarket>, LocalDateTime, LocalDateTime>> {
        return databaseClient.execute()
            .sql("SELECT * FROM poloniex_completed_transactions")
            .fetch().all()
            .map {
                tuple(
                    it["id"] as Long,
                    mapper.readValue<Array<TranIntentMarket>>(it["markets"] as String),
                    it["created_ts"] as LocalDateTime,
                    it["completed_ts"] as LocalDateTime
                )
            }
            .collectList()
            .awaitFirstOrDefault(emptyList())
    }

    suspend fun addCompleted(activeTranId: UUID, markets: Array<TranIntentMarket>) {
        val marketsJson = mapper
            .writerFor(jacksonTypeRef<Array<TranIntentMarket>>())
            .withView(Views.DB::class.java)
            .writeValueAsString(markets)

        databaseClient.execute()
            .sql("INSERT INTO poloniex_completed_transactions(created_ts, markets) VALUES ((SELECT created_ts FROM poloniex_active_transactions WHERE id = $1), $2)")
            .bind(0, activeTranId)
            .bind(1, marketsJson)
            .then()
            .awaitFirstOrNull()
    }

    suspend fun balanceInUse(currency: Currency): Tuple2<Currency, BigDecimal>? {
        return databaseClient.execute()
            .sql("SELECT from_currency, from_amount FROM poloniex_active_transactions WHERE from_currency = $1")
            .bind(0, currency)
            .fetch().one()
            .map {
                tuple(
                    it["from_currency"] as Currency,
                    it["from_amount"] as BigDecimal
                )
            }
            .awaitFirstOrNull()
    }

    suspend fun balancesInUse(currencies: io.vavr.collection.List<Currency>): List<Tuple2<Currency, BigDecimal>> {
        // TODO: Escape input and wait until driver will support List input
        val currencyList = currencies.toVavrStream().map { "'$it'" }.joinToString()

        return databaseClient.execute()
            .sql("SELECT from_currency, SUM(from_amount) amount FROM poloniex_active_transactions WHERE from_currency IN ($currencyList) GROUP BY from_currency")
            .fetch().all()
            .map {
                tuple(
                    it["from_currency"] as Currency,
                    it["amount"] as BigDecimal
                )
            }
            .collectList()
            .awaitFirstOrDefault(emptyList())
    }
}