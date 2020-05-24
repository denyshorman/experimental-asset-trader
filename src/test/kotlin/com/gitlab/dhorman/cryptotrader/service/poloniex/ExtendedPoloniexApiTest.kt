package com.gitlab.dhorman.cryptotrader.service.poloniex

import com.gitlab.dhorman.cryptotrader.core.toMarket
import com.gitlab.dhorman.cryptotrader.util.TestClock
import com.gitlab.dhorman.cryptotrader.util.first
import com.gitlab.dhorman.cryptotrader.util.firstOrNull
import io.vavr.kotlin.getOrNull
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runBlockingTest
import mu.KotlinLogging
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import java.time.Clock
import java.time.Instant
import java.time.ZoneId

@SpringBootTest
class ExtendedPoloniexApiTest {
    private val logger = KotlinLogging.logger {}

    @Autowired
    private lateinit var poloniexApi: ExtendedPoloniexApi

    @Autowired
    private lateinit var clock: Clock

    @Test
    fun `Get orderBooksPollingStream`() {
        runBlocking {
            val res = poloniexApi.orderBooksPollingStream.firstOrNull()
            println(res)
        }
    }

    @Test
    fun `Sync test for ExtendedPoloniexApi`() {
        runBlocking {
            launch {
                try {
                    delay(1000)
                    logger.info("trying to cancel order")
                    poloniexApi.cancelOrder(1)
                    logger.info("order has been cancelled")
                } catch (e: Throwable) {
                    logger.error(e.message)
                }
            }

            launch {
                poloniexApi.balanceStream.onStart {
                    logger.info("Subscribed to balance stream")
                }.collect {
                    logger.info("Balance update received $it")
                }
            }
        }
    }

    @Test
    fun `Test trade volume statistics for specified market`() {
        runBlockingTest {
            pauseDispatcher {
                val virtualClock = clock as TestClock
                val instant = Instant.parse("2020-05-17T00:00:00.00Z")
                virtualClock.setInstant(instant)
                val market = "USDT_BTC".toMarket()
                val stat = poloniexApi.tradeVolumeStat.first().getOrNull(market)?.first()
                logger.info(stat?.toString() ?: "Stat for market $market not found")
                virtualClock.setInstant(instant.plusSeconds(10 * 60))
                advanceTimeBy(10 * 60 * 1000)
                delay(2000)
                val stat2 = poloniexApi.tradeVolumeStat.first().getOrNull(market)?.first()
                logger.info(stat2?.toString() ?: "Stat for market $market not found")
            }
        }
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

class ExtendedPoloniexApiStreamSynchronizerTest {
    @Test
    fun streamSynchronizerTest() {
        runBlocking(Dispatchers.Default) {
            val ss = ExtendedPoloniexApi.Companion.StreamSynchronizer()

            launch {
                println("before func1")
                ss.withLockFunc {
                    println("in func1 start")
                    delay(2000)
                    println("in func1 end")
                }
                println("after func1")
            }

            launch {
                println("before stream1")
                delay(1000)
                ss.lockStream("x")
                ss.lockStream("x")
                println("in stream1 start")
                delay(5000)
                println("in stream1 end")
                ss.unlockStream("x")
                ss.unlockStream("x")
                ss.unlockStream("x")
                //yield()
                ss.lockStream("x")
                println("stream1 locked")
                ss.unlockStream("x")
                println("after stream1")
            }

            launch {
                println("before func2")
                delay(3000)
                ss.withLockFunc {
                    println("in func2 start")
                    delay(2000)
                    println("in func2 end")
                }
                println("after func2")
            }
        }
    }
}
