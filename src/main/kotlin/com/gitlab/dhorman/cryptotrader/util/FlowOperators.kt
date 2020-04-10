package com.gitlab.dhorman.cryptotrader.util

import io.vavr.Tuple2
import io.vavr.collection.Map
import io.vavr.collection.HashMap
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.flow.*
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
    private val upstreamSubscriptionLock = Mutex()
    private val channel = BroadcastChannel<T>(Channel.BUFFERED)
    private var upstreamSubscriptionJob: Job? = null
    private val queue: LinkedList<T>?
    private val queueLock: Mutex?
    private var gracePeriodTimerJob: Job? = null

    init {
        if (replayCount > 0) {
            queue = LinkedList()
            queueLock = Mutex()
        } else {
            queue = null
            queueLock = null
        }
    }

    private suspend fun cancelUpstreamSubscriptionJob() {
        upstreamSubscriptionJob?.cancelAndJoin()
        upstreamSubscriptionJob = null
        queue?.clear()
    }

    val shareOperator = channelFlow {
        try {
            coroutineScope {
                launch(start = CoroutineStart.UNDISPATCHED) {
                    if (replayCount > 0) {
                        queueLock?.withLock {
                            queue?.forEach {
                                this@channelFlow.send(it)
                            }
                        }
                    }

                    this@ShareOperator.channel.consumeEach {
                        this@channelFlow.send(it)
                    }
                }

                upstreamSubscriptionLock.withLock {
                    if (++subscribers == 1L) {
                        gracePeriodTimerJob?.cancelAndJoin()

                        if (upstreamSubscriptionJob == null) {
                            upstreamSubscriptionJob = GlobalScope.launch {
                                upstream.collect {
                                    if (queue != null) {
                                        queueLock?.withLock {
                                            queue.addLast(it)
                                            if (queue.size > replayCount) queue.removeFirst()
                                        }
                                    }

                                    this@ShareOperator.channel.send(it)
                                }

                                logger.warn("Downstream flow completed in share operator $upstream")
                            }
                        }
                    }
                }
            }
        } finally {
            withContext(NonCancellable) {
                upstreamSubscriptionLock.withLock {
                    if (--subscribers == 0L) {
                        if (gracePeriod != null && scope != null && scope.isActive) {
                            val launched = AtomicBoolean(false)

                            gracePeriodTimerJob = scope.launch(start = CoroutineStart.UNDISPATCHED) {
                                try {
                                    launched.set(true)
                                    delay(gracePeriod.toMillis())
                                } catch (ignored: Throwable) {
                                }

                                if (!scope.isActive || this.isActive) {
                                    withContext(NonCancellable) {
                                        cancelUpstreamSubscriptionJob()
                                    }
                                }
                            }

                            if (!launched.get()) cancelUpstreamSubscriptionJob()
                        } else {
                            cancelUpstreamSubscriptionJob()
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
