package com.gitlab.dhorman.cryptotrader.util.limiter

class SimpleRequestLimiter(private val allowedRequests: Int, private val perIntervalMs: Long) {
    private var reqCountFromFirst = 0L
    private var firstReqExecTime = 0L
    private var lastReqExecTime = 0L

    fun waitMs(): Long {
        val currReqTime = System.currentTimeMillis()

        if (currReqTime - lastReqExecTime > perIntervalMs) {
            reqCountFromFirst = 0
            firstReqExecTime = currReqTime
            lastReqExecTime = currReqTime
        } else {
            val offset = perIntervalMs * (reqCountFromFirst / allowedRequests)
            lastReqExecTime = firstReqExecTime + offset
        }

        reqCountFromFirst += 1

        return run {
            val d = lastReqExecTime - currReqTime
            if (d <= 0) 0 else d
        }
    }
}
