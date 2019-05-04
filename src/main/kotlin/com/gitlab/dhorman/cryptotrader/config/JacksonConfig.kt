package com.gitlab.dhorman.cryptotrader.config

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule
import com.gitlab.dhorman.cryptotrader.core.Market
import io.vavr.jackson.datatype.VavrModule
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class JacksonConfig {
    @Bean
    fun defaultObjectMapper(): ObjectMapper {
        val mapper = ObjectMapper()
            .registerModule(KotlinModule())
            .registerModule(VavrModule())
            .registerModule(ParameterNamesModule())
            .registerModule(Jdk8Module())
            .registerModule(JavaTimeModule())

        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true)
        mapper.configure(JsonParser.Feature.ALLOW_TRAILING_COMMA, true)

        // TODO: Extract module
        val simpleModule = SimpleModule()
        simpleModule.addKeyDeserializer(Market::class.java, Market.Companion.KeyDecoder())
        mapper.registerModule(simpleModule)

        return mapper
    }
}