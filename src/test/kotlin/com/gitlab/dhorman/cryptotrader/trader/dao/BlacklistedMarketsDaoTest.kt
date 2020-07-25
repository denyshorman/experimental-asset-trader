package com.gitlab.dhorman.cryptotrader.trader.dao

import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.util.TestClock
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import kotlin.test.assertEquals

@SpringBootTest
class BlacklistedMarketsDaoTest {
    @Autowired
    private lateinit var blacklistedMarkets: BlacklistedMarketsDao

    @Autowired
    private lateinit var clock: Clock

    @Test
    fun getAll() = runBlocking {
        val virtualClock = clock as TestClock
        val instant = Instant.parse("2000-01-01T00:00:00.00Z")
        virtualClock.setInstant(instant)
        val ttlSec = 30
        blacklistedMarkets.add(Market("BTC", "GRIN"), ttlSec)
        blacklistedMarkets.add(Market("BTC", "ETH"), ttlSec)
        var markets = blacklistedMarkets.getAll()
        assertEquals(2, markets.size())
        virtualClock.setInstant(instant.plusSeconds(100))
        markets = blacklistedMarkets.getAll()
        assertEquals(0, markets.size())
    }

    @Test
    @Disabled("Requires to add NOTIFY code")
    fun getAllWithUpdates() = runBlocking {
        val virtualClock = clock as TestClock
        val instant = Instant.parse("2000-01-01T00:00:00.00Z")
        virtualClock.setInstant(instant)
        val ttlSec = 30
        blacklistedMarkets.add(Market("BTC", "GRIN"), ttlSec)
        var markets = blacklistedMarkets.getAll()
        assertEquals(1, markets.size())
        delay(10000)
        markets = blacklistedMarkets.getAll()
        assertEquals(2, markets.size())
    }

    @TestConfiguration
    class ClockConfiguration {
        @Bean
        @Primary
        fun testClock(): Clock {
            return TestClock(Instant.now(), ZoneId.systemDefault())
        }
    }
}


@SpringBootTest
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class BlacklistedMarketsDbDaoTest {
    @Autowired
    private lateinit var blacklistedMarkets: BlacklistedMarketsDbDao

    @Test
    @Order(2)
    fun getAll() = runBlocking {
        val markets = blacklistedMarkets.getAll()
        println(markets)
    }

    @Test
    @Order(1)
    fun upsert() = runBlocking {
        val now = Instant.now()
        val ttl = 3600
        blacklistedMarkets.upsert(Market("BTC", "GRIN"), now, ttl)
        blacklistedMarkets.upsert(Market("BTC", "ETH"), now, ttl)
    }

    @Test
    @Order(3)
    fun remove() = runBlocking {
        blacklistedMarkets.remove(
            listOf(
                Market("BTC", "GRIN"),
                Market("BTC", "ETH")
            )
        )
    }

    @Test
    @Order(4)
    @Disabled("Requires to add NOTIFY code")
    fun testUpdates() = runBlocking {
        val n = blacklistedMarkets.updates.first()
        println("notification received $n")
    }
}
