package com.gitlab.dhorman.cryptotrader.service.poloniex

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onStart
import mu.KotlinLogging
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest
class ExtendedPoloniexApiTest {
    private val logger = KotlinLogging.logger {}

    @Autowired
    private lateinit var poloniexApi: ExtendedPoloniexApi

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
