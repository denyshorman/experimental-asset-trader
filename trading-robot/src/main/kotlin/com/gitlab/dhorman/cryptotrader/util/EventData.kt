package com.gitlab.dhorman.cryptotrader.util

data class EventData<out T>(
    val payload: T? = null,
    val subscribed: Boolean = false,
    val error: Throwable? = null,
)

fun <T, R> EventData<T>.newPayload(payload: R? = null) = EventData(payload, subscribed, error)
fun <T> EventData<T>.setPayload(payload: T?) = EventData(payload, subscribed, null)
fun <T> EventData<T>.setSubscribed(subscribed: Boolean) = EventData(payload, subscribed, null)
fun <T> EventData<T>.setError(error: Throwable?) = EventData<T>(null, false, error)
