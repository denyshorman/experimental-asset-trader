package com.gitlab.dhorman.cryptotrader.ui.crossexchangefuturestrader

import ch.qos.logback.classic.Level
import javafx.application.Application
import org.slf4j.Logger
import org.slf4j.LoggerFactory

fun main(args: Array<String>) {
    val rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
    rootLogger.level = Level.INFO

    val cryptoTraderLogger = LoggerFactory.getLogger("com.gitlab.dhorman.cryptotrader") as ch.qos.logback.classic.Logger
    cryptoTraderLogger.level = Level.DEBUG

    Application.launch(View::class.java, *args)
}
