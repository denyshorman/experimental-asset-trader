package com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.config

import com.gitlab.dhorman.cryptotrader.util.Secrets
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

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
}
