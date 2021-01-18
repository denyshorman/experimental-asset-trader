package com.gitlab.dhorman.cryptotrader.util.serializer

import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.math.BigDecimal

object NullableLongAsStringSerializer : KSerializer<Long?> {
    override val descriptor: SerialDescriptor = Long.serializer().nullable.descriptor

    override fun deserialize(decoder: Decoder): Long? {
        return if (decoder.decodeNotNullMark()) {
            decoder.decodeString().toLongOrNull()
        } else {
            decoder.decodeNull()
        }
    }

    override fun serialize(encoder: Encoder, value: Long?) {
        if (value == null) {
            encoder.encodeNull()
        } else {
            encoder.encodeString(value.toString())
        }
    }
}
