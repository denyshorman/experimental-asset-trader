package com.gitlab.dhorman.cryptotrader.util

inline fun ignoreErrors(body: () -> Unit) {
    try {
        body()
    } catch (e: Throwable) {
        // ignore error
    }
}
