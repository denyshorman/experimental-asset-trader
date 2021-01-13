package com.gitlab.dhorman.cryptotrader.util

import io.netty.channel.ChannelOption
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import io.netty.handler.timeout.ReadTimeoutHandler
import io.netty.handler.timeout.WriteTimeoutHandler
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient
import org.springframework.web.reactive.socket.client.WebSocketClient
import reactor.netty.http.client.HttpClient
import reactor.netty.http.client.WebsocketClientSpec
import reactor.netty.transport.ProxyProvider
import java.util.concurrent.TimeUnit

fun nettyHttpClient(
    connectTimeoutMs: Long? = 5000,
    readTimeoutMs: Long? = 5000,
    writeTimeoutMs: Long? = 5000,
): HttpClient {
    var client = HttpClient.create()
        .headers { it[HttpHeaderNames.USER_AGENT] = "trading-robot" }
        .secure {
            val sslContextBuilder = SslContextBuilder.forClient()

            if (System.getenv("HTTP_CERT_TRUST_ALL") != null) {
                sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE)
            }

            it.sslContext(sslContextBuilder.build())
        }
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs?.toInt())
        .doOnConnected { conn ->
            if (readTimeoutMs != null) conn.addHandlerLast(ReadTimeoutHandler(readTimeoutMs, TimeUnit.MILLISECONDS))
            if (writeTimeoutMs != null) conn.addHandlerLast(WriteTimeoutHandler(writeTimeoutMs, TimeUnit.MILLISECONDS))
        }

    if (System.getenv("HTTP_PROXY_ENABLED") != null) {
        client = client.proxy {
            val tpe = when (System.getenv("HTTP_PROXY_TYPE")) {
                "http" -> ProxyProvider.Proxy.HTTP
                "socks5" -> ProxyProvider.Proxy.SOCKS5
                else -> throw RuntimeException("Can't recognize HTTP_PROXY_TYPE option")
            }

            val host = System.getenv("HTTP_PROXY_HOST")
                ?: throw Exception("Please define HTTP_PROXY_HOST env variable")

            val port = System.getenv("HTTP_PROXY_PORT")?.toInt()
                ?: throw Exception("Please define valid port number for HTTP_PROXY_PORT env variable")

            it.type(tpe).host(host).port(port)
        }
    }

    return client
}

fun springWebClient(
    connectTimeoutMs: Long,
    readTimeoutMs: Long,
    writeTimeoutMs: Long,
    maxInMemorySize: Int,
): WebClient {
    return WebClient.builder()
        .clientConnector(ReactorClientHttpConnector(nettyHttpClient(connectTimeoutMs, readTimeoutMs, writeTimeoutMs)))
        .codecs {
            it.defaultCodecs().maxInMemorySize(maxInMemorySize)
        }
        .build()
}

fun springWebsocketClient(
    connectTimeoutMs: Long,
    readTimeoutMs: Long,
    writeTimeoutMs: Long,
    maxFramePayloadLength: Int,
): WebSocketClient {
    return ReactorNettyWebSocketClient(nettyHttpClient(connectTimeoutMs, readTimeoutMs, writeTimeoutMs)) {
        WebsocketClientSpec.builder()
            .maxFramePayloadLength(maxFramePayloadLength)
    }
}
