package com.gitlab.dhorman.cryptotrader.config

import com.gitlab.dhorman.cryptotrader.service.poloniexfutures.PoloniexFuturesApi
import com.gitlab.dhorman.cryptotrader.util.Secrets
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary

@Configuration
class PoloniexConfig {
    @Bean("POLONIEX_API_KEY")
    fun poloniexApiKey(): String {
        return Secrets.get("POLONIEX_API_KEY")
            ?: throw RuntimeException("Please define POLONIEX_API_KEY environment variable")
    }

    @Bean("POLONIEX_API_SECRET")
    fun poloniexApiSecret(): String {
        return Secrets.get("POLONIEX_API_SECRET")
            ?: throw RuntimeException("Please define POLONIEX_API_SECRET environment variable")
    }

    @Bean("POLONIEX_FUTURES_API_KEY")
    fun poloniexFuturesApiKey(): String {
        return Secrets.get("POLONIEX_FUTURES_API_KEY")
            ?: throw RuntimeException("Please define POLONIEX_FUTURES_API_KEY environment variable")
    }

    @Bean("POLONIEX_FUTURES_API_SECRET")
    fun poloniexFuturesApiSecret(): String {
        return Secrets.get("POLONIEX_FUTURES_API_SECRET")
            ?: throw RuntimeException("Please define POLONIEX_FUTURES_API_SECRET environment variable")
    }

    @Bean("POLONIEX_FUTURES_API_PASSPHRASE")
    fun poloniexFuturesApiPassphrase(): String {
        return Secrets.get("POLONIEX_FUTURES_API_PASSPHRASE")
            ?: throw RuntimeException("Please define POLONIEX_FUTURES_API_PASSPHRASE environment variable")
    }

    @Bean
    @Primary
    fun poloniexFuturesApi(): PoloniexFuturesApi {
        return PoloniexFuturesApi(
            apiKey = poloniexFuturesApiKey(),
            apiSecret = poloniexFuturesApiSecret(),
            apiPassphrase = poloniexFuturesApiPassphrase(),
        )
    }
}
