package com.gitlab.dhorman.cryptotrader.trader.dao

import com.gitlab.dhorman.cryptotrader.trader.PoloniexTrader
import com.gitlab.dhorman.cryptotrader.trader.TranIntentMarket
import io.vavr.collection.Array
import io.vavr.collection.Traversable
import org.springframework.data.r2dbc.function.DatabaseClient
import org.springframework.stereotype.Repository

@Repository
class TransactionsDao(private val databaseClient: DatabaseClient) {
    suspend fun getAll(): List<PoloniexTrader.TransactionIntent> {
        TODO()
    }

    suspend fun add(markets: Array<TranIntentMarket>): Long {
        TODO()
    }

    suspend fun delete(id: Long) {
        TODO()
    }

    suspend fun update(tran: PoloniexTrader.TransactionIntent) {
        TODO()
    }

    suspend fun getOrderIds(tranId: Long): Traversable<Long> {
        TODO()
    }

    suspend fun updateOrderIds(tranId: Long, orderIds: Traversable<Long>) {
        TODO()
    }
}