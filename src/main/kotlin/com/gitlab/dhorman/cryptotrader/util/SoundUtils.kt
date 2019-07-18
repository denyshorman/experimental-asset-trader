package com.gitlab.dhorman.cryptotrader.util

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import javax.sound.sampled.AudioFormat
import javax.sound.sampled.AudioSystem
import javax.sound.sampled.LineUnavailableException
import javax.sound.sampled.SourceDataLine
import kotlin.math.sin

object SoundUtils {
    private var SAMPLE_RATE = 8000f

    @Throws(LineUnavailableException::class)
    private fun tone(hz: Int, msecs: Int, vol: Double = 1.0) {
        val buf = ByteArray(1)
        val af = AudioFormat(SAMPLE_RATE, 8, 1, true, false)
        val sdl = AudioSystem.getSourceDataLine(af)
        sdl.open(af)
        sdl.start()
        for (i in 0 until msecs * 8) {
            val angle = (i / (SAMPLE_RATE / hz)).toDouble() * 2.0 * Math.PI
            buf[0] = (sin(angle) * 127.0 * vol).toByte()
            sdl.write(buf, 0, 1)
        }
        sdl.drain()
        sdl.stop()
        sdl.close()
    }

    fun beep() {
        GlobalScope.launch(Dispatchers.IO) { tone(1000, 1000) }
    }
}
