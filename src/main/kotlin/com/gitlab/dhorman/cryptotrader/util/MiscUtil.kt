package com.gitlab.dhorman.cryptotrader.util

import kotlinx.serialization.json.JsonObject

inline fun ignoreErrors(body: () -> Unit) {
    try {
        body()
    } catch (e: Throwable) {
        // ignore error
    }
}

val emptyJsonObject = JsonObject(emptyMap())
