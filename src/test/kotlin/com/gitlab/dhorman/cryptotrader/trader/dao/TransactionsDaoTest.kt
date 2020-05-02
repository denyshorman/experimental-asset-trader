package com.gitlab.dhorman.cryptotrader.trader.dao

import com.gitlab.dhorman.cryptotrader.trader.DataStreams
import com.gitlab.dhorman.cryptotrader.trader.core.AdjustedPoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketCompleted
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketExtensions
import io.vavr.kotlin.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TransactionsDaoTest {
    @Autowired
    private lateinit var transactionsDao: TransactionsDao

    @Autowired
    private lateinit var dataStreams: DataStreams

    @Autowired
    private lateinit var amountCalculator: AdjustedPoloniexBuySellAmountCalculator

    private lateinit var marketExtensions: TranIntentMarketExtensions

    @BeforeAll
    fun setup() {
        marketExtensions = TranIntentMarketExtensions(amountCalculator, dataStreams)
    }

    @Test
    fun getCompleted() = runBlocking {
        val completedTransactions = transactionsDao.getCompleted()

        val transactionsStr = completedTransactions.joinToString(",", "[", "]") { (id, markets, createTime, completeTime) ->
            val marketsStr = markets.joinToString(",", "[", "]") { market ->
                market as TranIntentMarketCompleted
                """{"market":"${market.market}","fromAmount":"${marketExtensions.fromAmount(market)}","targetAmount":"${marketExtensions.targetAmount(market)}","speed":"${market.orderSpeed}"}"""
            }
            val fromAmount = marketExtensions.fromAmount(markets[0] as TranIntentMarketCompleted)
            val targetAmount = marketExtensions.targetAmount(markets[markets.size() - 1] as TranIntentMarketCompleted)
            """{"id":$id,"fromAmount":"$fromAmount","targetAmount":"$targetAmount","createTime":"$createTime","completeTime":"$completeTime","markets":$marketsStr}"""
        }

        println(transactionsStr)
    }
}
