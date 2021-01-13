package com.gitlab.dhorman.cryptotrader.trader.dao

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest
class SettingsDaoTest {
    @Autowired
    private lateinit var settingsDao: SettingsDao

    @Test
    fun getPrimaryCurrenciesTest() {
        runBlocking {
            val primaryCurrencies = settingsDao.getPrimaryCurrencies()
            println(primaryCurrencies)
        }
    }

    @Test
    fun getFixedAmountTest() {
        runBlocking {
            val fixedAmount = settingsDao.getFixedAmount()
            println(fixedAmount)
        }
    }

    @Test
    fun getMinTradeAmountTest() {
        runBlocking {
            val minTradeAmount = settingsDao.getMinTradeAmount()
            println(minTradeAmount)
        }
    }

    @Test
    fun getBlacklistMarketTimeTest() {
        runBlocking {
            val blacklistMarketTime = settingsDao.getBlacklistMarketTime()
            println(blacklistMarketTime)
        }
    }
}


@SpringBootTest
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class SettingsDbDaoTest {
    @Autowired
    private lateinit var settingsDbDao: SettingsDbDao

    @Test
    @Order(2)
    fun getAll() = runBlocking {
        val settings = settingsDbDao.getAll()
        println(settings)
    }

    @Test
    @Order(1)
    fun upsert() = runBlocking {
        settingsDbDao.upsert("test", "test")
    }

    @Test
    @Order(3)
    fun remove() = runBlocking {
        settingsDbDao.remove("test")
    }

    @Test
    @Order(4)
    fun testUpdates() = runBlocking {
        val n = settingsDbDao.updates.first()
        println("notification received $n")
    }
}
