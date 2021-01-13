package com.gitlab.dhorman.cryptotrader.util.serializer

import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

object BooleanAsStringSerializer : KSerializer<Boolean> {
    override val descriptor: SerialDescriptor = String.serializer().descriptor

    override fun deserialize(decoder: Decoder): Boolean {
        return when (val value = decoder.decodeString()) {
            "true" -> true
            "false" -> false
            else -> throw SerializationException("Can't decode boolean $value")
        }
    }

    override fun serialize(encoder: Encoder, value: Boolean) {
        encoder.encodeString(value.toString())
    }
}
