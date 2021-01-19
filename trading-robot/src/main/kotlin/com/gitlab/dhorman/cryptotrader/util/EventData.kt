package com.gitlab.dhorman.cryptotrader.util

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.transform

data class EventData<out T>(
    val payload: T? = null,
    val subscribed: Boolean = false,
    val error: Throwable? = null,
)

fun <T, R> EventData<T>.newPayload(payload: R? = null) = EventData(payload, subscribed, error)
fun <T> EventData<T>.setPayload(payload: T?) = EventData(payload, subscribed, null)
fun <T> EventData<T>.setSubscribed(subscribed: Boolean) = EventData(payload, subscribed, null)
fun <T> EventData<T>.setError(error: Throwable?) = EventData<T>(null, false, error)

fun <T> EventData<T>.unsubscribed() = EventData<T>(null, false, null)
fun <T> EventData<T>.subscribed() = EventData<T>(null, true, null)

fun <T> Flow<EventData<T>>.onlyPayload() = transform { event -> if (event.payload != null) emit(event.payload) }
