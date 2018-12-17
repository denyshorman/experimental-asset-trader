package com.gitlab.dhorman.cryptotrader.config

import io.vertx.scala.core.{Vertx, http}
import io.vertx.scala.ext.bridge.PermittedOptions
import io.vertx.scala.ext.web.Router
import io.vertx.scala.ext.web.handler.{ErrorHandler, LoggerHandler, ResponseContentTypeHandler}
import io.vertx.scala.ext.web.handler.sockjs.{BridgeOptions, SockJSHandler}

class HttpServer(private val vertx: Vertx) {
  val server: http.HttpServer = vertx.createHttpServer()
  val router: Router = Router.router(vertx)

  router.route("/api/*")
    .handler(LoggerHandler.create())
    .handler(ResponseContentTypeHandler.create())
    .failureHandler(ErrorHandler.create())

  private val options = BridgeOptions()
    .addOutboundPermitted(PermittedOptions()
      .setAddress("values"))

  private val sockJSHandler = SockJSHandler.create(vertx).bridge(options)

  router.route("/eventbus/*").handler(sockJSHandler)

  router.get("/api/values").handler(rc => {
    rc.response().end("""{"resp":"hello"}""")
  })

  server.requestHandler(router).listen(8080)
}
