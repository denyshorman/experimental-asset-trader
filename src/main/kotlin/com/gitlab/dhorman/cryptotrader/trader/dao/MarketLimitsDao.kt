package com.gitlab.dhorman.cryptotrader.trader.dao

import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Price
import io.r2dbc.postgresql.api.PostgresqlConnection
import io.r2dbc.spi.ConnectionFactory
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactive.collect
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.stereotype.Repository
import java.math.BigDecimal

@Repository
class MarketLimitsDao(private val marketLimitsDao: MarketLimitsCachedDao) {
    suspend fun getAll(): List<MarketLimit> {
        return marketLimitsDao.getAll()
    }

    suspend fun setAll(marketLimits: List<MarketLimit>) {
        marketLimitsDao.setAll(marketLimits)
    }

    suspend fun getAllBaseCurrencyLimits(): Map<Currency, Amount> {
        return marketLimitsDao.getAllBaseCurrencyLimits()
    }
}

@Repository
class MarketLimitsCachedDao(private val marketLimitsDao: MarketLimitsDbDao) {
    private val logger = KotlinLogging.logger {}

    @Volatile
    private var initialized = false

    @Volatile
    private var marketLimits = emptyList<MarketLimit>()

    @Volatile
    private var marketBaseCurrencyLimits = emptyMap<Currency, Amount>()

    private val mutex = Mutex()

    suspend fun getAll(): List<MarketLimit> {
        initCache()
        return marketLimits
    }

    suspend fun getAllBaseCurrencyLimits(): Map<Currency, Amount> {
        initCache()
        return marketBaseCurrencyLimits
    }

    suspend fun setAll(marketLimits: List<MarketLimit>) {
        initCache()
        mutex.withLock {
            this.marketLimits = marketLimits
            indexData()
            marketLimitsDao.setAll(marketLimits)
        }
    }

    private fun indexData() {
        val marketBaseCurrencyLimitsMap = HashMap<Currency, Amount>(marketLimits.size)
        marketBaseCurrencyLimits = marketBaseCurrencyLimitsMap
        marketLimits.forEach { limit ->
            if (limit.total != null) {
                marketBaseCurrencyLimitsMap[limit.market.baseCurrency] = limit.total
            }
        }
    }

    private suspend fun initCache() {
        if (initialized) return
        mutex.withLock {
            if (initialized) return
            fetchAll()
            subscribeToUpdates()
            initialized = true
        }
    }

    private suspend fun fetchAll() {
        val marketLimits = marketLimitsDao.getAll()
        this.marketLimits = marketLimits
        indexData()
    }

    private fun subscribeToUpdates() {
        val job = GlobalScope.launch {
            marketLimitsDao.updates.collect {
                logger.debug("Poloniex market limits update has been received from database. Refreshing the cache...")
                mutex.withLock {
                    fetchAll()
                }
            }
        }

        Runtime.getRuntime().addShutdownHook(Thread {
            runBlocking {
                job.cancelAndJoin()
                logger.debug("Subscription to market limit updates has been cancelled")
            }
        })
    }
}

@Repository
class MarketLimitsDbDao(
    @Qualifier("pg_client") private val entityTemplate: R2dbcEntityTemplate,
    @Qualifier("pg_conn_factory") private val connectionFactory: ConnectionFactory
) {
    private val logger = KotlinLogging.logger {}

    val updates = channelFlow {
        while (isActive) {
            var connection: PostgresqlConnection? = null
            try {
                try {
                    connection = connectionFactory.create().awaitSingle() as PostgresqlConnection
                    connection.createStatement("LISTEN poloniex_market_limit_updates").execute().awaitSingle()
                    connection.notifications.collect { send(it) }
                } finally {
                    withContext(NonCancellable) {
                        connection?.close()?.awaitFirstOrNull()
                    }
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                logger.debug { "Error in MarketLimitsDbDao: ${e.message}" }
                delay(500)
            }
        }
    }

    suspend fun getAll(): List<MarketLimit> {
        return entityTemplate.databaseClient.sql("SELECT market_base_currency, market_quote_currency, price, amount, total FROM poloniex_market_limits")
            .fetch().all()
            .map {
                MarketLimit(
                    Market(
                        it["market_base_currency"] as String,
                        it["market_quote_currency"] as String
                    ),
                    it["price"] as BigDecimal?,
                    it["amount"] as BigDecimal?,
                    it["total"] as BigDecimal?
                )
            }
            .collectList()
            .awaitSingle()
    }

    suspend fun removeAll() {
        entityTemplate.databaseClient.sql("DELETE FROM poloniex_market_limits").then().awaitFirstOrNull()
    }

    suspend fun setAll(marketLimits: List<MarketLimit>) {
        if (marketLimits.isEmpty()) return

        val values = marketLimits.asSequence()
            .map { "('${it.market.baseCurrency}','${it.market.quoteCurrency}',${if (it.price == null) "null" else "'${it.price}'"},${if (it.amount == null) "null" else "'${it.amount}'"},${if (it.total == null) "null" else "'${it.total}'"})" }
            .joinToString(",")

        removeAll()

        entityTemplate.databaseClient.sql("INSERT INTO poloniex_market_limits(market_base_currency, market_quote_currency, price, amount, total) VALUES $values").then().awaitFirstOrNull()
    }
}

data class MarketLimit(
    val market: Market,
    val price: Price?,
    val amount: Amount?,
    val total: BigDecimal?
)
