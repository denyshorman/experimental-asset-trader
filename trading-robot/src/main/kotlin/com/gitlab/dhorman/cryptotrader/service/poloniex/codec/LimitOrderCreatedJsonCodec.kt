package com.gitlab.dhorman.cryptotrader.service.poloniex.codec

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.kotlin.treeToValue
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.LimitOrderCreated
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object LimitOrderCreatedJsonCodec {
    private val df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    class Decoder : JsonDeserializer<LimitOrderCreated>() {
        override fun deserialize(p: JsonParser, ctx: DeserializationContext): LimitOrderCreated {
            val arrayNode: ArrayNode = p.readValueAsTree()
            val codec = p.codec as ObjectMapper
            return LimitOrderCreated(
                arrayNode[1].asInt(),
                arrayNode[2].asLong(),
                codec.treeToValue(arrayNode[3])!!,
                codec.treeToValue(arrayNode[4])!!,
                codec.treeToValue(arrayNode[5])!!,
                LocalDateTime.parse(arrayNode[6].asText(), df),
                codec.treeToValue(arrayNode[7])!!,
                arrayNode[8].let { if (it.isNull) null else it.asLong() }
            )
        }
    }
}
