package com.gitlab.dhorman.cryptotrader.util

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousCloseException
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.CompletionHandler
import java.nio.channels.FileLock
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

suspend fun AsynchronousFileChannel.aLock() = suspendCoroutine<FileLock> { cont ->
    lock(cont, asyncIOHandler())
}

suspend fun AsynchronousFileChannel.aLock(
    position: Long,
    size: Long,
    shared: Boolean
) = suspendCoroutine<FileLock> { cont ->
    lock(position, size, shared, cont, asyncIOHandler())
}

suspend fun AsynchronousFileChannel.aRead(
    buf: ByteBuffer,
    position: Long
) = suspendCoroutine<Int> { cont ->
    read(buf, position, cont, asyncIOHandler())
}

suspend fun AsynchronousFileChannel.readString(bufSize: Int = 256): String {
    val buf = ByteBuffer.allocate(bufSize)
    val sb = StringBuilder()
    var pos = 0
    while (true) {
        val readCount = aRead(buf, pos.toLong())
        if (readCount == -1) break
        pos += readCount
        sb.append(String(buf.array(), 0, readCount))
        buf.clear()
    }
    return sb.toString()
}

suspend fun AsynchronousFileChannel.aWrite(
    buf: ByteBuffer,
    position: Long
) = suspendCoroutine<Int> { cont ->
    write(buf, position, cont, asyncIOHandler())
}

suspend fun AsynchronousFileChannel.writeString(data: String) {
    withContext(Dispatchers.IO) { truncate(0) }
    val buf = ByteBuffer.wrap(data.toByteArray())
    aWrite(buf, 0)
}

private object AsyncIOHandlerAny : CompletionHandler<Any, Continuation<Any>> {
    override fun completed(result: Any, cont: Continuation<Any>) {
        cont.resume(result)
    }

    override fun failed(ex: Throwable, cont: Continuation<Any>) {
        if (ex is AsynchronousCloseException) return
        cont.resumeWithException(ex)
    }
}

@Suppress("UNCHECKED_CAST")
private fun <T> asyncIOHandler() = AsyncIOHandlerAny as CompletionHandler<T, Continuation<T>>
