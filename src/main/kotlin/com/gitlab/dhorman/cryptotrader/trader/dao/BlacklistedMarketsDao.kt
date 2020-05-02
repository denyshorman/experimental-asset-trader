package com.gitlab.dhorman.cryptotrader.trader.dao

import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.core.toMarket
import io.r2dbc.postgresql.api.PostgresqlConnection
import io.r2dbc.spi.ConnectionFactory
import io.vavr.Tuple2
import io.vavr.Tuple3
import io.vavr.collection.HashMap
import io.vavr.collection.HashSet
import io.vavr.collection.Map
import io.vavr.collection.Set
import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import io.vavr.kotlin.toVavrStream
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
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Repository
import java.time.Clock
import java.time.Instant
import java.time.OffsetDateTime

@Repository
class BlacklistedMarketsDao(
    private val blacklistedMarkets: BlacklistedMarketsCachedDao,
    private val clock: Clock
) {
    private val mutex = Mutex()

    suspend fun getAll(): Set<Market> {
        mutex.withLock {
            val markets = blacklistedMarkets.getAll()
            val now = Instant.now(clock)

            var goodMarkets = HashSet.empty<Market>()
            val marketDeleteCandidates = mutableListOf<Market>()

            markets.forEach { (market, value) ->
                val (addedTimestamp, ttlSec) = value
                val endTimestamp = addedTimestamp.plusSeconds(ttlSec.toLong())
                if (now.isAfter(endTimestamp)) {
                    marketDeleteCandidates.add(market)
                } else {
                    goodMarkets = goodMarkets.add(market)
                }
            }

            if (marketDeleteCandidates.isNotEmpty()) {
                blacklistedMarkets.remove(marketDeleteCandidates)
            }

            return goodMarkets
        }
    }

    suspend fun add(market: Market, ttlSec: Int) {
        mutex.withLock {
            blacklistedMarkets.upsert(market, ttlSec)
        }
    }
}

@Repository
class BlacklistedMarketsCachedDao(
    private val blacklistedMarkets: BlacklistedMarketsDbDao,
    private val clock: Clock
) {
    private val logger = KotlinLogging.logger {}

    @Volatile
    private var initialized = false

    @Volatile
    private var markets: Map<Market, Tuple2<Instant, Int>> = HashMap.empty()

    private val mutex = Mutex()

    suspend fun getAll(): Map<Market, Tuple2<Instant, Int>> {
        initCache()
        return markets
    }

    suspend fun upsert(market: Market, ttlSec: Int) {
        initCache()
        val now = Instant.now(clock)
        mutex.withLock {
            blacklistedMarkets.upsert(market, now, ttlSec)
            markets = markets.put(market, tuple(now, ttlSec))
        }
    }

    suspend fun remove(markets: Iterable<Market>) {
        initCache()
        mutex.withLock {
            blacklistedMarkets.remove(markets)
            this.markets = this.markets.removeAll(markets)
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
        val dbMarkets = blacklistedMarkets.getAll()
        markets = HashMap.ofEntries(dbMarkets.toVavrStream().map { tuple(it._1, tuple(it._2, it._3)) })
    }

    private fun subscribeToUpdates() {
        val job = GlobalScope.launch {
            blacklistedMarkets.updates.collect {
                logger.debug("Poloniex blacklisted market update has been received from database. Refreshing the cache...")

                mutex.withLock {
                    fetchAll()
                }
            }
        }

        Runtime.getRuntime().addShutdownHook(Thread {
            runBlocking {
                job.cancelAndJoin()
                logger.debug("Subscription to Poloniex blacklisted market updates has been cancelled")
            }
        })
    }
}

@Repository
class BlacklistedMarketsDbDao(
    @Qualifier("pg_client") private val databaseClient: DatabaseClient,
    @Qualifier("pg_conn_factory") private val connectionFactory: ConnectionFactory
) {
    private val logger = KotlinLogging.logger {}

    val updates = channelFlow {
        while (isActive) {
            var connection: PostgresqlConnection? = null
            try {
                try {
                    connection = connectionFactory.create().awaitSingle() as PostgresqlConnection
                    connection.createStatement("LISTEN poloniex_blacklisted_markets_updates").execute().awaitSingle()
                    connection.notifications.collect { send(it) }
                } finally {
                    withContext(NonCancellable) {
                        connection?.close()?.awaitFirstOrNull()
                    }
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                logger.debug { "Error in BlacklistedMarketsDbDao: ${e.message}" }
                delay(500)
            }
        }
    }

    suspend fun getAll(): List<Tuple3<Market, Instant, Int>> {
        return databaseClient.execute("SELECT market, added_ts, ttl_sec FROM poloniex_blacklisted_markets ")
            .fetch().all()
            .map {
                tuple(
                    (it["market"] as String).toMarket(),
                    (it["added_ts"] as OffsetDateTime).toInstant(),
                    it["ttl_sec"] as Int
                )
            }
            .collectList()
            .awaitSingle()
    }

    suspend fun upsert(market: Market, upsertTimestamp: Instant, ttlSec: Int) {
        databaseClient.execute("INSERT INTO poloniex_blacklisted_markets(market) VALUES ($1) ON CONFLICT (market) DO UPDATE SET added_ts = $2, ttl_sec = $3 ")
            .bind(0, market.toString())
            .bind(1, upsertTimestamp)
            .bind(2, ttlSec)
            .then()
            .awaitFirstOrNull()
    }

    suspend fun remove(markets: Iterable<Market>) {
        val marketsSqlString = markets.joinToString(",") { "'$it'" } // TODO: Remove when r2dbc postgres adds support from IN stmt

        databaseClient.execute("DELETE FROM poloniex_blacklisted_markets WHERE market IN ($marketsSqlString)")
            .then()
            .awaitFirstOrNull()
    }
}
