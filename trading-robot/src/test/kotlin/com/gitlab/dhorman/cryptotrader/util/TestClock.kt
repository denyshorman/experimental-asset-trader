package com.gitlab.dhorman.cryptotrader.util

import java.time.Clock
import java.time.Instant
import java.time.ZoneId

class TestClock(private val startTime: Instant, private val zone: ZoneId) : Clock() {
    @Volatile
    private var instantValue = startTime

    override fun withZone(zone: ZoneId): Clock {
        return fixed(startTime, zone)
    }

    override fun getZone(): ZoneId {
        return zone
    }

    override fun instant(): Instant {
        return instantValue
    }

    fun setInstant(instantValue: Instant) {
        this.instantValue = instantValue
    }
}
