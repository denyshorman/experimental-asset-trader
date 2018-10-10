package com.gitlab.dhorman.cryptotrader

import com.gitlab.dhorman.cryptotrader.service.PoloniexApi
import com.typesafe.scalalogging.Logger
import io.vertx.reactivex.core.Vertx

case class Command(command: String, channel: Int)

object Command {

  object Type {
    val Subscribe = "subscribe"
    val Unsubscribe = "unsubscribe"
  }

  object Channel {
    val AccountNotifications = 1000
    val TickerData = 1002
    val _24HourExchangeVolume = 1003
    val Heartbeat = 1010
  }

}

object Main extends App {
  val vertx: Vertx = Vertx.vertx()
  val logger = Logger[Main.type]

  val p = new PoloniexApi(vertx)

  p.tickerStream().subscribe(json => {
    logger.info(json.noSpaces)
  }, err => {
    logger.error(err.toString)
  }, () => {
    logger.info("websocket connection completed")
  })
}
