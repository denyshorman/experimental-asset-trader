package com.gitlab.dhorman.cryptotrader.util

import java.util.concurrent.CountDownLatch

import com.typesafe.scalalogging.Logger
import org.scalatest.FlatSpec
import reactor.core.scala.publisher.Flux

import scala.concurrent.duration._

class RequestLimiterSpec extends FlatSpec {
  private val logger = Logger[RequestLimiterSpec]

  "RequestLimiter" should "correctly sync N requests in specified time interval" in {
    val reqLimiter = new RequestLimiter(2, 3.seconds)
    val reqCount = 10
    val additionalReqCount = 5
    val additionalReqSleepCount = Array(2000, 1000, 14000, 1000, 1000)
    val latch = new CountDownLatch(reqCount + additionalReqCount)

    def sendRequest(id: Int): Unit = {
      Flux.just(id).delaySubscription(reqLimiter.get()).subscribe(v => {
        logger.info(s"received: $v")
        latch.countDown()
      })
    }

    for (i <- 0 until reqCount) {
      sendRequest(i)
    }

    for (i <- 0 until additionalReqCount) {
      Thread.sleep(additionalReqSleepCount(i))
      sendRequest(reqCount + i)
    }

    latch.await()
  }

}
