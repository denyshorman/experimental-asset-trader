package com.gitlab.dhorman.cryptotrader.util

import scala.concurrent.duration._

class RequestLimiter(allowedRequests: Int, perInterval: Duration) {
  private var reqCountFromFirst: Long = 0
  private var firstReqExecTime: Long = 0
  private var lastReqExecTime: Long = 0

  def get(): Duration = this.synchronized {
    val currReqTime = System.currentTimeMillis

    if (currReqTime - lastReqExecTime > perInterval.toMillis) {
      reqCountFromFirst = 0
      firstReqExecTime = currReqTime
      lastReqExecTime = currReqTime
    } else {
      val offset = perInterval.mul(reqCountFromFirst / allowedRequests).toMillis
      lastReqExecTime = firstReqExecTime + offset
    }

    reqCountFromFirst += 1

    val delay = {
      val d = lastReqExecTime - currReqTime
      if (d <= 0) 0 else d
    }

    Duration(delay, MILLISECONDS)
  }
}
