package com.gitlab.dhorman.cryptotrader.service

import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.diContainer
import com.gitlab.dhorman.cryptotrader.service.poloniex.PoloniexApi
import io.vavr.concurrent.Promise
import mu.KotlinLogging
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import org.kodein.di.erased.instance

class PoloniexApiSpec {
    private val logger = KotlinLogging.logger {}
    private val poloniexApi: PoloniexApi by diContainer.instance()

    @Test
    fun `PoloniexApi ticker stream should return some amount of data and completes successfully`() {
        val p = Promise.make<Unit>()

        poloniexApi.tickerStream.take(10)
            .doOnTerminate {
                p.success(Unit)
            }
            .subscribe({ ticker ->
                logger.info(ticker.toString())
            }, { err ->
                logger.error(err.toString(), err)
                fail(err)
            })

        p.future().await()
    }

    @Test
    fun `PoloniexApi order book stream should return order book and updates`() {
        val p = Promise.make<Unit>()

        val usdcUsdt = 226

        poloniexApi.orderBookStream(usdcUsdt)
            .doOnTerminate {
                p.success(Unit)
            }
            .subscribe({ book ->
                logger.info(book.toString())
            }, { err ->
                logger.error(err.toString(), err)
                fail(err)
            })

        p.future().await()
    }

    @Test
    fun `PoloniexApi _24HourExchangeVolumeStream should return one 24 hours exchange volume`() {
        val p = Promise.make<Unit>()

        poloniexApi.dayExchangeVolumeStream.take(1)
            .doOnTerminate {
                p.success(Unit)
            }
            .subscribe({ dayVolume ->
                logger.info(dayVolume.toString())
            }, { err ->
                logger.error(err.toString(), err)
                fail(err)
            })

        p.future().await()
    }

    @Test
    fun `PoloniexApi accountNotificationStream should return account related notifications`() {
        val p = Promise.make<Unit>()

        poloniexApi.accountNotificationStream
            .doOnTerminate {
                p.success(Unit)
            }
            .subscribe({ notification ->
                logger.info(notification.toString())
            }, { err ->
                err.printStackTrace()
                logger.error(err.toString(), err)
                fail(err)
            })

        p.future().await()
    }

    @Test
    fun `PoloniexApi returnBalances should return some value`() {
        val p = Promise.make<Unit>()

        poloniexApi.availableBalances()
            .doOnTerminate {
                p.success(Unit)
            }
            .subscribe({ balances ->
                logger.info(balances.toString())
            }, { err ->
                logger.error(err.toString(), err)
                fail(err)
            })

        p.future().await()
    }

    @Test
    fun `PoloniexApi returnCompleteBalances should return some value`() {
        val p = Promise.make<Unit>()

        poloniexApi.completeBalances()
            .doOnTerminate {
                p.success(Unit)
            }
            .subscribe({ balances ->
                logger.info(balances.toString())
            }, { err ->
                logger.error(err.toString(), err)
                fail(err)
            })

        p.future().await()
    }

    @Test
    fun `PoloniexApi openOrders should return some value`() {
        val p = Promise.make<Unit>()

        poloniexApi.openOrders(Market("ETH", "OMG"))
            .doOnTerminate {
                p.success(Unit)
            }
            .subscribe({ address ->
                logger.info(address.toString())
            }, { err ->
                logger.error(err.toString(), err)
                fail(err)
            })

        p.future().await()
    }

    @Test
    fun `PoloniexApi allOpenOrders should return some value`() {
        val p = Promise.make<Unit>()

        poloniexApi.allOpenOrders()
            .doOnTerminate {
                p.success(Unit)
            }
            .subscribe({ address ->
                logger.info(address.toString())
            }, { err ->
                logger.error(err.toString(), err)
                fail(err)
            })

        p.future().await()
    }

    @Test
    fun `PoloniexApi currencies should return all currencies available`() {
        val p = Promise.make<Unit>()

        poloniexApi.currencies()
            .doOnTerminate {
                p.success(Unit)
            }
            .subscribe({ currencies ->
                logger.info(currencies.toString())
            }, { err ->
                logger.error(err.toString(), err)
                fail(err)
            })

        p.future().await()
    }

    @Test
    fun `PoloniexApi feeInfo should return all fees`() {
        val p = Promise.make<Unit>()

        poloniexApi.feeInfo()
            .doOnTerminate {
                p.success(Unit)
            }
            .subscribe({ fees ->
                logger.info(fees.toString())
            }, { err ->
                logger.error(err.toString(), err)
                fail(err)
            })

        p.future().await()
    }

    @Test
    fun `PoloniexApi tradeHistoryPublic should return all publish trade history`() {
        val p = Promise.make<Unit>()

        poloniexApi.tradeHistoryPublic(Market("USDT", "BTC"))
            .doOnTerminate {
                p.success(Unit)
            }
            .subscribe({ history ->
                logger.info(history.toString())
            }, { err ->
                logger.error(err.toString(), err)
                fail(err)
            })

        p.future().await()
    }
}
