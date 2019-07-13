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

private val logger = KotlinLogging.logger {}

private open class ShareOperator<T>(
    private val upstream: Flow<T>,
    private val replayCount: Int = 0,
    private val gracePeriod: Duration? = null
) : AbstractFlow<T>() {
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

    override suspend fun collectSafely(collector: FlowCollector<T>) {
        try {
            coroutineScope {
                launch {
                    if (replayCount > 0) {
                        queueLock?.withLock {
                            withContext(this@coroutineScope.coroutineContext) {
                                queue?.forEach {
                                    collector.emit(it)
                                }
                            }
                        }
                    }

                    channel.consumeEach {
                        withContext(this@coroutineScope.coroutineContext) {
                            collector.emit(it)
                        }
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

                                    channel.send(it)
                                }

                                logger.warn("Downstream flow completed in share operator")
                            }
                        }
                    }
                }
            }
        } finally {
            withContext(NonCancellable) {
                upstreamSubscriptionLock.withLock {
                    if (--subscribers == 0L) {
                        if (gracePeriod != null) {
                            gracePeriodTimerJob = GlobalScope.launch {
                                delay(gracePeriod.toMillis())

                                withContext(NonCancellable) {
                                    upstreamSubscriptionJob?.cancelAndJoin()
                                    upstreamSubscriptionJob = null
                                }
                            }
                        } else {
                            upstreamSubscriptionJob?.cancelAndJoin()
                            upstreamSubscriptionJob = null
                        }
                    }
                }
            }
        }
    }
}

fun <T> Flow<T>.share(replayCount: Int = 0, gracePeriod: Duration? = null): Flow<T> {
    return ShareOperator(this, replayCount, gracePeriod)
}

fun <T> Flow<T>.buffer(timespan: Duration): Flow<List<T>> = channelFlow {
    coroutineScope {
        var events = LinkedList<T>()
        var timerJob: Job? = null
        val lock = Mutex()

        collect {
            lock.withLock {
                if (timerJob == null) {
                    timerJob = launch {
                        delay(timespan.toMillis())

                        lock.withLock {
                            send(events)

                            events = LinkedList()
                            timerJob = null
                        }
                    }
                }

                events.add(it)
            }
        }

        timerJob?.cancelAndJoin()
        if (events.size != 0) send(events)
    }
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
