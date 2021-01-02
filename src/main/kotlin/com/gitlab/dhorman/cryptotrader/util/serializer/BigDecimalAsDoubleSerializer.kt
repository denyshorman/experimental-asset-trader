package com.gitlab.dhorman.cryptotrader.util.serializer

import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.math.BigDecimal

object BigDecimalAsDoubleSerializer : KSerializer<BigDecimal> {
    override val descriptor: SerialDescriptor = Double.serializer().descriptor

    override fun deserialize(decoder: Decoder): BigDecimal {
        return BigDecimal(decoder.decodeDouble().toString())
    }

    override fun serialize(encoder: Encoder, value: BigDecimal) {
        encoder.encodeDouble(value.toDouble())
    }
}
