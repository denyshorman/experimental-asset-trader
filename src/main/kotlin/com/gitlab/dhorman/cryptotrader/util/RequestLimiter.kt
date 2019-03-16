package com.gitlab.dhorman.cryptotrader.util

import java.time.Duration

class RequestLimiter(private val allowedRequests: Int, private val perInterval: Duration) {
    private var reqCountFromFirst = 0L
    private var firstReqExecTime = 0L
    private var lastReqExecTime = 0L

    @Synchronized
    fun get(): Duration {
        val currReqTime = System.currentTimeMillis()

        if (currReqTime - lastReqExecTime > perInterval.toMillis()) {
            reqCountFromFirst = 0
            firstReqExecTime = currReqTime
            lastReqExecTime = currReqTime
        } else {
            val offset = perInterval.multipliedBy(reqCountFromFirst / allowedRequests).toMillis()
            lastReqExecTime = firstReqExecTime + offset
        }

        reqCountFromFirst += 1

        val delay = run {
            val d = lastReqExecTime - currReqTime
            if (d <= 0) 0 else d
        }

        return Duration.ofMillis(delay)
    }
}
