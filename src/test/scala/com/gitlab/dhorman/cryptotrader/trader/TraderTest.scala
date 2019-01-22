package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.TestModule
import org.scalatest.FlatSpec

class TraderTest extends FlatSpec with TestModule {

  "Trader" should "correctly subscribe unsubscribe subscribe from tickers stream" in {
    val dis = trader.data.tickers.subscribe()
    Thread.sleep(2000)
    dis.dispose()
    Thread.sleep(1000)
    trader.data.tickers.subscribe()
  }

}
