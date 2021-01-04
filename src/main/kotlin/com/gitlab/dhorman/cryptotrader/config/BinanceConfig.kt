package com.gitlab.dhorman.cryptotrader.config

import com.gitlab.dhorman.cryptotrader.service.binance.BinanceApi
import com.gitlab.dhorman.cryptotrader.service.binance.BinanceFuturesApi
import com.gitlab.dhorman.cryptotrader.util.Secrets
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.context.annotation.Profile

@Configuration
class BinanceConfig {
    private fun binanceApiKey(): String {
        return Secrets.get("BINANCE_API_KEY")
            ?: throw RuntimeException("Please define BINANCE_API_KEY environment variable")
    }

    private fun binanceApiSecret(): String {
        return Secrets.get("BINANCE_API_SECRET")
            ?: throw RuntimeException("Please define BINANCE_API_SECRET environment variable")
    }

    private fun binanceTestNetApiKey(): String {
        return Secrets.get("BINANCE_TEST_NET_API_KEY")
            ?: throw RuntimeException("Please define BINANCE_TEST_NET_API_KEY environment variable")
    }

    private fun binanceTestNetApiSecret(): String {
        return Secrets.get("BINANCE_TEST_NET_API_SECRET")
            ?: throw RuntimeException("Please define BINANCE_TEST_NET_API_SECRET environment variable")
    }


    private fun binanceFuturesApiKey(): String {
        return Secrets.get("BINANCE_FUTURES_API_KEY")
            ?: throw RuntimeException("Please define BINANCE_FUTURES_API_KEY environment variable")
    }

    private fun binanceFuturesApiSecret(): String {
        return Secrets.get("BINANCE_FUTURES_API_SECRET")
            ?: throw RuntimeException("Please define BINANCE_FUTURES_API_SECRET environment variable")
    }

    private fun binanceFuturesTestNetApiKey(): String {
        return Secrets.get("BINANCE_FUTURES_TEST_NET_API_KEY")
            ?: throw RuntimeException("Please define BINANCE_FUTURES_TEST_NET_API_KEY environment variable")
    }

    private fun binanceFuturesTestNetApiSecret(): String {
        return Secrets.get("BINANCE_FUTURES_TEST_NET_API_SECRET")
            ?: throw RuntimeException("Please define BINANCE_FUTURES_TEST_NET_API_SECRET environment variable")
    }

    @Bean
    @Primary
    fun binanceMainNetApi(): BinanceApi {
        return BinanceApi(
            apiKey = binanceApiKey(),
            apiSecret = binanceApiSecret(),
            apiNet = BinanceApi.ApiNet.Main,
        )
    }

    @Bean
    @Profile("test")
    fun binanceTestNetApi(): BinanceApi {
        return BinanceApi(
            apiKey = binanceTestNetApiKey(),
            apiSecret = binanceTestNetApiSecret(),
            apiNet = BinanceApi.ApiNet.Test,
        )
    }

    @Bean
    @Primary
    fun binanceFuturesMainNetApi(): BinanceFuturesApi {
        return BinanceFuturesApi(
            apiKey = binanceFuturesApiKey(),
            apiSecret = binanceFuturesApiSecret(),
            apiNet = BinanceFuturesApi.ApiNet.Main,
        )
    }

    @Bean
    @Profile("test")
    fun binanceFuturesTestNetApi(): BinanceFuturesApi {
        return BinanceFuturesApi(
            apiKey = binanceFuturesTestNetApiKey(),
            apiSecret = binanceFuturesTestNetApiSecret(),
            apiNet = BinanceFuturesApi.ApiNet.Test,
        )
    }
}
