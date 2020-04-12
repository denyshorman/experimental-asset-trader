package com.gitlab.dhorman.cryptotrader.util

import io.vavr.Tuple2
import io.vavr.collection.HashMap
import io.vavr.collection.Map
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import mu.KotlinLogging
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

private val logger = KotlinLogging.logger {}

private open class ShareOperator<T>(
    private val upstream: Flow<T>,
    private val replayCount: Int = 0,
    private val gracePeriod: Duration? = null,
    private val scope: CoroutineScope? = null
) {
    private var subscribers = 0L
    private val subscriberChannels = mutableSetOf<SendChannel<T>>()
    private val subscriberChannelsMutex = Mutex()
    private val upstreamSubscriptionLock = Mutex()
    private var upstreamSubscriptionJob: Job? = null
    private val queue = if (replayCount > 0) LinkedList<T>() else null
    private var gracePeriodTimerJob: Job? = null

    private suspend fun subscribeToUpstream() {
        if (upstreamSubscriptionJob != null) return

        upstreamSubscriptionJob = GlobalScope.launch {
            upstream.collect { data ->
                subscriberChannelsMutex.withLock {
                    if (queue != null) {
                        queue.addLast(data)
                        if (queue.size > replayCount) queue.removeFirst()
                    }

                    subscriberChannels.forEach { subscriber ->
                        try {
                            subscriber.send(data)
                        } catch (_: Throwable) {
                        }
                    }
                }
            }

            logger.warn("Downstream flow completed in share operator $upstream")

            // TODO: Handle complete event
        }
    }

    private suspend fun cancelUpstreamSubscription() {
        upstreamSubscriptionJob?.cancelAndJoin()
        upstreamSubscriptionJob = null
        queue?.clear()
    }

    private suspend fun launchGracePeriodTimerJob(): Boolean {
        if (gracePeriod == null || scope == null || !scope.isActive) return false

        val launched = AtomicBoolean(false)

        gracePeriodTimerJob = scope.launch(start = CoroutineStart.UNDISPATCHED) {
            try {
                launched.set(true)
                delay(gracePeriod.toMillis())
            } catch (_: Throwable) {
            }

            if (!scope.isActive || this.isActive) {
                withContext(NonCancellable) {
                    cancelUpstreamSubscription()
                }
            }
        }

        return launched.get()
    }

    private suspend fun cancelGracePeriodTimerJob() {
        gracePeriodTimerJob?.cancelAndJoin()
    }

    private suspend fun ProducerScope<T>.processQueueAndSubscribeSelf() {
        subscriberChannelsMutex.withLock {
            queue?.forEach {
                try {
                    send(it)
                } catch (_: Throwable) {
                }
            }

            subscriberChannels.add(channel)
        }
    }

    private suspend fun ProducerScope<T>.unsubscribeSelf() {
        subscriberChannelsMutex.withLock {
            subscriberChannels.remove(channel)
        }
    }

    val shareOperator = channelFlow<T> {
        try {
            upstreamSubscriptionLock.withLock {
                if (++subscribers == 1L) {
                    cancelGracePeriodTimerJob()
                    processQueueAndSubscribeSelf()
                    subscribeToUpstream()
                } else {
                    processQueueAndSubscribeSelf()
                }
            }

            delay(Long.MAX_VALUE)
        } finally {
            withContext(NonCancellable) {
                upstreamSubscriptionLock.withLock {
                    unsubscribeSelf()

                    if (--subscribers == 0L) {
                        val launched = launchGracePeriodTimerJob()

                        if (!launched) {
                            cancelUpstreamSubscription()
                        }
                    }
                }
            }
        }
    }
}

fun <T> Flow<T>.share(replayCount: Int = 0, gracePeriod: Duration? = null, scope: CoroutineScope? = null): Flow<T> {
    return ShareOperator(this, replayCount, gracePeriod, scope).shareOperator
}

fun <T> Channel<T>.buffer(scope: CoroutineScope, timespan: Duration): Channel<List<T>> {
    val upstream = Channel<List<T>>(Channel.RENDEZVOUS)
    var events = LinkedList<T>()
    var timerJob: Job? = null
    val lock = Mutex()

    scope.launch {
        var error: Throwable? = null

        try {
            consumeEach {
                lock.withLock {
                    if (timerJob == null) {
                        timerJob = launch {
                            delay(timespan.toMillis())

                            lock.withLock {
                                upstream.send(events)

                                events = LinkedList()
                                timerJob = null
                            }
                        }
                    }

                    events.add(it)
                }
            }
        } catch (e: Throwable) {
            error = e
        } finally {
            withContext(NonCancellable) {
                timerJob?.cancelAndJoin()
                if (events.size != 0) upstream.send(events)
                upstream.close(error)
            }
        }
    }

    return upstream
}

fun <K, V> flowFromMap(map: Map<K, V>): Flow<Tuple2<K, V>> = flow {
    map.forEach { emit(it) }
}

suspend fun <K, V> Flow<Tuple2<K, V>>.collectMap(): Map<K, V> {
    var map = HashMap.empty<K, V>()

    collect {
        map = map.put(it)
    }

    return map
}

suspend fun <T> Flow<T>.firstOrNull(): T? {
    var result: T? = null

    try {
        collect {
            result = it
            throw CancellationException()
        }
    } catch (e: CancellationException) {
        // Ignore
    }

    return result
}

fun <T> Channel<T>.asFlow(): Flow<T> = flow {
    consumeEach { emit(it) }
}

fun <T> Flow<T>.returnLastIfNoValueWithinSpecifiedTime(duration: Duration) = channelFlow {
    val lastValue = AtomicReference<T>(null)
    val delayMillis = duration.toMillis()
    var timeoutJob: Job? = null

    collect { value ->
        timeoutJob?.cancelAndJoin()
        lastValue.set(value)
        timeoutJob = launch(start = CoroutineStart.UNDISPATCHED) {
            delay(delayMillis)
            send(lastValue.get())
        }
        send(value)
    }
}
