package com.gitlab.dhorman.cryptotrader

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule
import com.gitlab.dhorman.cryptotrader.config.HttpServer
import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.service.poloniex.PoloniexApi
import com.gitlab.dhorman.cryptotrader.trader.Trader
import com.gitlab.dhorman.cryptotrader.util.Secrets
import io.vavr.jackson.datatype.VavrModule
import io.vertx.core.Vertx
import io.vertx.core.json.Json
import io.vertx.reactivex.RxHelper
import org.kodein.di.Kodein
import org.kodein.di.erased.bind
import org.kodein.di.erased.instance
import org.kodein.di.erased.singleton
import reactor.adapter.rxjava.RxJava2Scheduler
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers

val diContainer = Kodein {

    bind<ObjectMapper>() with singleton {
        val mapper = ObjectMapper()
            .registerModule(KotlinModule())
            .registerModule(VavrModule())
            .registerModule(ParameterNamesModule())
            .registerModule(Jdk8Module())
            .registerModule(JavaTimeModule())

        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true)
        mapper.configure(JsonParser.Feature.ALLOW_TRAILING_COMMA, true)

        val simpleModule = SimpleModule()
        simpleModule.addKeyDeserializer(Market::class.java, Market.Companion.KeyDecoder())
        mapper.registerModule(simpleModule)

        mapper
    }

    bind<Vertx>() with singleton {
        Json.mapper = instance()
        Json.prettyMapper = instance<ObjectMapper>().copy()

        Json.prettyMapper.configure(SerializationFeature.INDENT_OUTPUT, true)

        Vertx.vertx()
    }

    bind<Scheduler>(tag = "vertxScheduler") with singleton {
        RxJava2Scheduler.from(RxHelper.scheduler(instance<Vertx>()))
    }

    bind<Schedulers.Factory>(tag = "reactorSchedulersFactory") with singleton {
        object : Schedulers.Factory {
        }
    }

    bind<String>(tag = "poloniexApiKey") with singleton {
        Secrets.get("POLONIEX_API_KEY") ?: throw Exception("Please define POLONIEX_API_KEY environment variable")
    }

    bind<String>(tag = "poloniexApiSecret") with singleton {
        Secrets.get("POLONIEX_API_SECRET") ?: throw Exception("Please define POLONIEX_API_SECRET environment variable")
    }

    bind<PoloniexApi>() with singleton {
        PoloniexApi(
            instance(),
            instance(tag = "poloniexApiKey"),
            instance(tag = "poloniexApiSecret")
        )
    }

    bind<HttpServer>() with singleton {
        HttpServer(
            instance(),
            instance()
        )
    }

    bind<Trader>() with singleton {
        Trader(instance())
    }
}
