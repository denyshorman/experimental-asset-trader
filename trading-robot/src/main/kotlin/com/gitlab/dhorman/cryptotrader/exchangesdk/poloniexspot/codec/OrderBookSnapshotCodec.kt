package com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.codec

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.treeToValue
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.Amount
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.OrderBookSnapshot
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.Price
import io.vavr.collection.TreeMap
import java.math.BigDecimal

object OrderBookSnapshotCodec {
    class Decoder : JsonDeserializer<OrderBookSnapshot>() {
        override fun deserialize(p: JsonParser, ctx: DeserializationContext): OrderBookSnapshot {
            val codec = p.codec as ObjectMapper
            val objectNode: ObjectNode = p.readValueAsTree()

            var asks = TreeMap.empty<Price, Amount>()
            var bids = TreeMap.empty<Price, Amount>(compareByDescending { it })

            (objectNode["asks"] as ArrayNode).forEach { priceAmount ->
                priceAmount as ArrayNode
                val price = codec.treeToValue<BigDecimal>(priceAmount[0])!!
                val amount = codec.treeToValue<BigDecimal>(priceAmount[1])!!
                asks = asks.put(price, amount)
            }

            (objectNode["bids"] as ArrayNode).forEach { priceAmount ->
                priceAmount as ArrayNode
                val price = codec.treeToValue<BigDecimal>(priceAmount[0])!!
                val amount = codec.treeToValue<BigDecimal>(priceAmount[1])!!
                bids = bids.put(price, amount)
            }

            val isFrozenNode = objectNode["isFrozen"]

            val isFrozen = when {
                isFrozenNode.isBoolean -> isFrozenNode.booleanValue()
                isFrozenNode.isTextual -> when (val isFrozenTextValue = isFrozenNode.textValue()) {
                    "0" -> false
                    "1" -> true
                    "false" -> false
                    "true" -> true
                    else -> throw JsonMappingException(ctx.parser, "Not recognized text value of isFrozen field $isFrozenTextValue")
                }
                else -> throw JsonMappingException(ctx.parser, "Not recognized type of isFrozen field")
            }

            val snapshot = objectNode["seq"].longValue()

            return OrderBookSnapshot(asks, bids, isFrozen, snapshot)
        }
    }
}
