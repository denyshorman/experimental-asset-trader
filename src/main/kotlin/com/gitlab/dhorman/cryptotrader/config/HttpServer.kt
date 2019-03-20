package com.gitlab.dhorman.cryptotrader.config

import com.fasterxml.jackson.module.kotlin.readValue
import com.gitlab.dhorman.cryptotrader.trader.Trader
import io.vertx.core.Vertx
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.Json
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.ErrorHandler
import io.vertx.ext.web.handler.LoggerHandler
import io.vertx.ext.web.handler.ResponseContentTypeHandler
import mu.KotlinLogging

import reactor.core.Disposable
import reactor.core.publisher.Flux
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

class HttpServer(
    vertx: Vertx,
    trader: Trader
) {
    private val server = vertx.createHttpServer()
    private val router = Router.router(vertx)
    private val logger = KotlinLogging.logger {}

    init {
        router.route("/api/*")
            .handler(LoggerHandler.create())
            .handler(ResponseContentTypeHandler.create())
            .failureHandler(ErrorHandler.create())

        router.get("/api/values").handler {
            it.response().end("""{"resp":"hello"}""")
        }
    }

    private val streams = mapOf(
        MsgId.Ticker to trader.data.tickers
            .sample(Duration.ofSeconds(1))
            .map { RespMsg(MsgId.Ticker, it) }
            .map { Json.encode(it) }
            .share(),

        MsgId.Paths to trader.indicators.paths
            .cache(1)
            .sampleFirst(Duration.ofSeconds(30))
            .onBackpressureLatest()
            .flatMapSequential({ Flux.fromIterable(it).buffer(500) }, 1, 1)
            .map { RespMsg(MsgId.Paths, it) }
            .map { Json.encode(it) }
            .share()
    )

    private fun webSocketHandler(ws: ServerWebSocket) {
        if (ws.path() == "/api/ws") {
            logger.debug("websocket connection established")

            val map = ConcurrentHashMap<MsgId, Disposable>()

            ws.textMessageHandler { msg ->
                try {
                    val req = Json.mapper.readValue<ReqMsg>(msg)

                    when (req.type) {
                        MsgType.Subscribe -> {
                            if (logger.isDebugEnabled) logger.debug("subscribe to ${req.id}")

                            when (req.id) {
                                MsgId.Ticker -> {
                                    val disposable = streams.getValue(MsgId.Ticker).subscribe({ json ->
                                        ws.writeTextMessage(json)
                                    }, { e ->
                                        logger.error(e.message, e)
                                    })

                                    map[MsgId.Ticker] = disposable
                                }
                                MsgId.Paths -> {
                                    val disposable = streams.getValue(MsgId.Paths).subscribe({ json ->
                                        ws.writeTextMessage(json)
                                    }, { e ->
                                        logger.error(e.message, e)
                                    })

                                    map[MsgId.Paths] = disposable
                                }
                            }
                        }
                        MsgType.Unsubscribe -> {
                            val dis = map[req.id]
                            if (dis != null) {
                                map.remove(req.id)
                                dis.dispose()
                            }

                            if (logger.isDebugEnabled) logger.debug("unsubscribe from ${req.id}")
                        }
                    }
                } catch (e: Exception) {
                    logger.error(e.message, e)
                }
            }

            ws.closeHandler {
                logger.debug("websocket connection closed")
                map.forEach { _, d -> d.dispose() }
            }
        } else {
            ws.reject()
        }
    }

    fun start() {
        server.requestHandler(router)
            .websocketHandler(this::webSocketHandler)
            .listen(8080, "0.0.0.0")
    }
}

data class ReqMsg(val type: MsgType, val id: MsgId)

data class RespMsg<T>(val id: MsgId, val data: T)

enum class MsgType(val id: Byte) {
    Subscribe(0),
    Unsubscribe(1)
}

enum class MsgId(val id: Byte) {
    Ticker(0),
    Paths(2)
}
