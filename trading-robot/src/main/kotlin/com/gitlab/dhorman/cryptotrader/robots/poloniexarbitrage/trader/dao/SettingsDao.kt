package com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.dao

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.Amount
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.Currency
import io.r2dbc.postgresql.api.PostgresqlConnection
import io.r2dbc.spi.ConnectionFactory
import io.vavr.Tuple2
import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import io.vavr.kotlin.tuple
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
class SettingsDao(private val settingsCachedDao: SettingsCachedDao) {
    suspend fun getPrimaryCurrencies(): List<Currency> {
        return settingsCachedDao.getPrimaryCurrencies()
    }

    suspend fun getFixedAmount(): Map<Currency, Amount> {
        return settingsCachedDao.getFixedAmount()
    }

    suspend fun getMinTradeAmount(): BigDecimal {
        return settingsCachedDao.getMinTradeAmount()
    }

    suspend fun getBlacklistMarketTime(): Int {
        return settingsCachedDao.getBlacklistMarketTime()
    }

    suspend fun getCheckProfitabilityInterval(): Int {
        return settingsCachedDao.getCheckProfitabilityInterval()
    }

    suspend fun getCheckPathPriceThreshold(): BigDecimal {
        return settingsCachedDao.getCheckPathPriceThreshold()
    }

    suspend fun getUnfilledInitAmountThreshold(): BigDecimal {
        return settingsCachedDao.getUnfilledInitAmountThreshold()
    }

    suspend fun getTotalMustBeAtLeastThreshold(): BigDecimal {
        return settingsCachedDao.getTotalMustBeAtLeastThreshold()
    }
}

@Repository
class SettingsCachedDao(
    private val settingsDbDao: SettingsDbDao,
    private val mapper: ObjectMapper
) {
    private val logger = KotlinLogging.logger {}

    @Volatile
    private var initialized = false

    private val mutex = Mutex()

    private var _primaryCurrencies: List<Currency> = emptyList()
    private var _fixedAmount: Map<Currency, Amount> = emptyMap()
    private var _minTradeAmount: BigDecimal = BigDecimal.ZERO
    private var _blacklistMarketTime: Int = 0
    private var _checkProfitabilityInterval: Int = 0
    private var _checkPathPriceThreshold: BigDecimal = BigDecimal.ZERO
    private var _unfilledInitAmountThreshold: BigDecimal = BigDecimal.ZERO
    private var _totalMustBeAtLeastThreshold: BigDecimal = BigDecimal.ZERO

    suspend fun getPrimaryCurrencies(): List<Currency> {
        initCache()
        return _primaryCurrencies
    }

    suspend fun getFixedAmount(): Map<Currency, Amount> {
        initCache()
        return _fixedAmount
    }

    suspend fun getMinTradeAmount(): BigDecimal {
        initCache()
        return _minTradeAmount
    }

    suspend fun getBlacklistMarketTime(): Int {
        initCache()
        return _blacklistMarketTime
    }

    suspend fun getCheckProfitabilityInterval(): Int {
        initCache()
        return _checkProfitabilityInterval
    }

    suspend fun getCheckPathPriceThreshold(): BigDecimal {
        initCache()
        return _checkPathPriceThreshold
    }

    suspend fun getUnfilledInitAmountThreshold(): BigDecimal {
        initCache()
        return _unfilledInitAmountThreshold
    }

    suspend fun getTotalMustBeAtLeastThreshold(): BigDecimal {
        initCache()
        return _totalMustBeAtLeastThreshold
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
        val settings = settingsDbDao.getAll()
        for ((key, value) in settings) {
            updateCache(key, value)
        }
    }

    private fun subscribeToUpdates() {
        val job = GlobalScope.launch {
            settingsDbDao.updates.collect { notification ->
                mutex.withLock {
                    if (notification.parameter == null || notification.parameter == "all") {
                        fetchAll()
                        logger.info("Received update from Poloniex settings database for all keys")
                    } else {
                        val key = notification.parameter!!
                        val value = settingsDbDao.get(key)
                        if (value != null) updateCache(key, value)
                        logger.info("Received update from Poloniex settings database for key $key")
                    }
                }
            }
        }

        Runtime.getRuntime().addShutdownHook(Thread {
            runBlocking {
                job.cancelAndJoin()
                logger.debug("Subscription to Poloniex settings updates has been cancelled")
            }
        })
    }

    private fun updateCache(key: String, value: String) {
        when (key) {
            "primaryCurrencies" -> {
                try {
                    _primaryCurrencies = mapper.readValue(value)
                } catch (e: Throwable) {
                    logger.warn("Can't parse primaryCurrencies from database ${e.message}")
                }
            }
            "fixedCurrencyAmount" -> {
                try {
                    val currencyAmountArray = mapper.readValue<Array<Tuple2<Currency, Amount>>>(value)
                    val currencyAmountMap = mutableMapOf<Currency, Amount>()
                    for ((currency, amount) in currencyAmountArray) currencyAmountMap[currency] = amount
                    _fixedAmount = currencyAmountMap
                } catch (e: Throwable) {
                    logger.warn("Can't parse fixedCurrencyAmount from database ${e.message}")
                }
            }
            "minTradeAmount" -> {
                try {
                    _minTradeAmount = value.toBigDecimal()
                } catch (e: Throwable) {
                    logger.warn("Can't parse minTradeAmount from database ${e.message}")
                }
            }
            "blacklistMarketTime" -> {
                try {
                    _blacklistMarketTime = value.toInt()
                } catch (e: Throwable) {
                    logger.warn("Can't parse blacklistMarketTime from database ${e.message}")
                }
            }
            "checkProfitabilityInterval" -> {
                try {
                    _checkProfitabilityInterval = value.toInt()
                } catch (e: Throwable) {
                    logger.warn("Can't parse checkProfitabilityInterval from database ${e.message}")
                }
            }
            "checkPathPriceThreshold" -> {
                try {
                    _checkPathPriceThreshold = value.toBigDecimal()
                } catch (e: Throwable) {
                    logger.warn("Can't parse checkPathPriceThreshold from database ${e.message}")
                }
            }
            "unfilledInitAmountThreshold" -> {
                try {
                    _unfilledInitAmountThreshold = value.toBigDecimal()
                } catch (e: Throwable) {
                    logger.warn("Can't parse unfilledInitAmountThreshold from database ${e.message}")
                }
            }
            "totalMustBeAtLeastThreshold" -> {
                try {
                    _totalMustBeAtLeastThreshold = value.toBigDecimal()
                } catch (e: Throwable) {
                    logger.warn("Can't parse totalMustBeAtLeastThreshold from database ${e.message}")
                }
            }
            else -> {
                logger.warn("Can't update Poloniex settings cache because $key is not defined")
            }
        }
    }
}

@Repository
class SettingsDbDao(
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
                    connection.createStatement("LISTEN poloniex_settings_updates").execute().awaitSingle()
                    connection.notifications.collect { send(it) }
                } finally {
                    withContext(NonCancellable) {
                        connection?.close()?.awaitFirstOrNull()
                    }
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                logger.debug { "Error in SettingDbDao: ${e.message}" }
                delay(500)
            }
        }
    }

    suspend fun getAll(): List<Tuple2<String, String>> {
        return entityTemplate.databaseClient.sql("SELECT key,value FROM poloniex_settings")
            .fetch().all()
            .map { tuple(it["key"] as String, it["value"] as String) }
            .collectList()
            .awaitSingle()
    }

    suspend fun get(key: String): String? {
        return entityTemplate.databaseClient.sql("SELECT value FROM poloniex_settings where key = $1")
            .bind(0, key)
            .fetch().one()
            .map { it["value"] as String }
            .awaitFirstOrNull()
    }

    suspend fun upsert(key: String, value: String) {
        entityTemplate.databaseClient.sql("INSERT INTO poloniex_settings(key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value = $2")
            .bind(0, key)
            .bind(1, value)
            .then()
            .awaitFirstOrNull()
    }

    suspend fun remove(key: String) {
        entityTemplate.databaseClient.sql("DELETE FROM poloniex_settings WHERE key = $1")
            .bind(0, key)
            .then()
            .awaitFirstOrNull()
    }
}
