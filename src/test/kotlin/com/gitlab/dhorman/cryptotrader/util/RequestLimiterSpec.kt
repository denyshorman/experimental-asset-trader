package com.gitlab.dhorman.cryptotrader.util

import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import java.time.Duration
import java.util.concurrent.CountDownLatch


class RequestLimiterSpec {
    private val logger = KotlinLogging.logger {}

    @Test
    fun `RequestLimiter should correctly sync N requests in specified time interval`() = runBlocking {
        val reqLimiter = RequestLimiter(2, Duration.ofSeconds(3))
        val reqCount = 10
        val additionalReqCount = 5
        val additionalReqSleepCount = arrayOf<Long>(2000, 1000, 14000, 1000, 1000)
        val latch = CountDownLatch(reqCount + additionalReqCount)

        suspend fun sendRequest(id: Int) {
            Flux.just(id).delaySubscription(reqLimiter.get()).subscribe { v ->
                logger.info("received: $v")
                latch.countDown()
            }
        }

        for (i in 0 until reqCount) {
            sendRequest(i)
        }

        for (i in 0 until additionalReqCount) {
            Thread.sleep(additionalReqSleepCount[i])
            sendRequest(reqCount + i)
        }

        latch.await()
    }
}
