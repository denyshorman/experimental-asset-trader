package com.gitlab.dhorman.cryptotrader.util

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.serialization.json.JsonObject
import mu.KotlinLogging
import kotlin.time.Duration
import kotlin.time.milliseconds
import kotlin.time.seconds

private val logger = KotlinLogging.logger {}
val emptyJsonObject = JsonObject(emptyMap())

inline fun ignoreErrors(body: () -> Unit) {
    try {
        body()
    } catch (e: Throwable) {
        // ignore error
    }
}

suspend fun <T> infiniteRetry(
    minWait: Duration = 500.milliseconds,
    maxWait: Duration = 2.seconds,
    step: Duration = 500.milliseconds,
    func: suspend () -> T,
): T {
    var waitDuration = minWait.toLongMilliseconds()
    val maxWaitMillis = maxWait.toLongMilliseconds()
    val stepMillis = step.toLongMilliseconds()

    while (true) {
        try {
            return func()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            logger.warn { "Error occurred: ${e.message}" }
            delay(waitDuration)
            waitDuration += stepMillis
            if (waitDuration > maxWaitMillis) {
                waitDuration = maxWaitMillis
            }
        }
    }
}
