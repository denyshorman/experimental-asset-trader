package com.gitlab.dhorman.cryptotrader.config

import com.gitlab.dhorman.cryptotrader.service.binance.BinanceApi
import com.gitlab.dhorman.cryptotrader.util.Secrets
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
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

    private fun binanceFileCachePath(): String? {
        return Secrets.get("BINANCE_FILE_CACHE_PATH")
    }

    private fun binanceTestNetFileCachePath(): String? {
        return Secrets.get("BINANCE_TEST_NET_FILE_CACHE_PATH")
    }

    @Bean
    fun binanceMainNetApi(): BinanceApi {
        return BinanceApi(
            apiKey = binanceApiKey(),
            apiSecret = binanceApiSecret(),
            apiNet = BinanceApi.ApiNet.Main,
            fileCachePath = binanceFileCachePath()
        )
    }

    @Bean
    @Profile("test")
    fun binanceTestNetApi(): BinanceApi {
        return BinanceApi(
            apiKey = binanceTestNetApiKey(),
            apiSecret = binanceTestNetApiSecret(),
            apiNet = BinanceApi.ApiNet.Test,
            fileCachePath = binanceTestNetFileCachePath()
        )
    }
}
