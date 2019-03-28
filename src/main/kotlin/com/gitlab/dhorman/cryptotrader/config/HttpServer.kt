package com.gitlab.dhorman.cryptotrader.config

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import com.fasterxml.jackson.module.kotlin.readValue
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.trader.Trader
import com.gitlab.dhorman.cryptotrader.trader.indicator.paths.PathsSettings
import io.vavr.collection.List
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
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

class HttpServer(
    vertx: Vertx,
    private val trader: Trader
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
                                    if (req.params != null && req.params != NullNode.instance) {
                                        val params = Json.mapper.convertValue<PathParams>(
                                            req.params,
                                            jacksonTypeRef<PathParams>()
                                        )
                                        val settings = PathsSettings(params.initAmount, params.currencies)

                                        val disposable = trader.indicators.getPaths(settings)
                                            .sampleFirst(Duration.ofSeconds(30))
                                            .onBackpressureLatest()
                                            .flatMapSequential({
                                                Flux.fromIterable(it)
                                                    .buffer(250)
                                                    .subscribeOn(Schedulers.elastic())
                                            }, 1, 1)
                                            .map { RespMsg(MsgId.Paths, it) }
                                            .map { Json.encode(it) }
                                            .subscribe({ json ->
                                                ws.writeTextMessage(json)
                                            }, { e ->
                                                logger.error(e.message, e)
                                            })

                                        map[MsgId.Paths] = disposable
                                    }
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

data class ReqMsg(val type: MsgType, val id: MsgId, val params: JsonNode?)

data class RespMsg<T>(val id: MsgId, val data: T)

enum class MsgType(@get:JsonValue val id: Byte) {
    Subscribe(0),
    Unsubscribe(1);

    companion object {
        @JsonCreator
        @JvmStatic
        fun valueById(id: Byte) = MsgId.values().find { it.id == id }
    }
}

enum class MsgId(@get:JsonValue val id: Byte) {
    Ticker(0),
    Paths(2);

    companion object {
        @JsonCreator
        @JvmStatic
        fun valueById(id: Byte) = values().find { it.id == id }
    }
}

data class PathParams(
    val initAmount: Amount,
    val currencies: List<Currency>
)