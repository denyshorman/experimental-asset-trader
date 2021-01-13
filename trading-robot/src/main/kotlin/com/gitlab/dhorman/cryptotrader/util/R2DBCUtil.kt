package com.gitlab.dhorman.cryptotrader.util

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactor.mono
import mu.KotlinLogging
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.reactive.TransactionalOperator

private val logger = KotlinLogging.logger {}

private val logTranErrorFun = { e: Throwable ->
    logger.debug { "Error occurred in transaction: ${e.message}. Retrying..." }
}

private val repeatableReadTranDef = object : TransactionDefinition {
    override fun getIsolationLevel() = TransactionDefinition.ISOLATION_REPEATABLE_READ
}

suspend fun <T> ReactiveTransactionManager.defaultTran(block: suspend CoroutineScope.() -> T): T {
    val tranOperator = TransactionalOperator.create(this)
    val blockMono = mono(Dispatchers.Unconfined, block)
    return tranOperator.transactional(blockMono).doOnError(logTranErrorFun).retry().awaitFirst()
}

suspend fun <T> ReactiveTransactionManager.repeatableReadTran(block: suspend CoroutineScope.() -> T): T {
    val tranOperator = TransactionalOperator.create(this, repeatableReadTranDef)
    val blockMono = mono(Dispatchers.Unconfined, block)
    return tranOperator.transactional(blockMono).doOnError(logTranErrorFun).retry().awaitFirst()
}
