package com.gitlab.dhorman.cryptotrader.config

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationFeature
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
    fun coreJacksonModule(): SimpleModule {
        val module = SimpleModule()
        module.addKeyDeserializer(Market::class.java, Market.Companion.KeyDecoder())
        return module
    }

    @Bean
    fun defaultObjectMapper(modules: List<SimpleModule>): ObjectMapper {
        val mapper = ObjectMapper()
            .registerModule(KotlinModule())
            .registerModule(VavrModule())
            .registerModule(ParameterNamesModule())
            .registerModule(Jdk8Module())
            .registerModule(JavaTimeModule())

        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true)

        if (System.getenv("JACKSON_FAIL_ON_UNKNOWN_PROPERTIES") == null) {
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        } else {
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
        }

        modules.forEach { module ->
            mapper.registerModule(module)
        }

        return mapper
    }
}
