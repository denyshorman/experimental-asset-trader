package com.gitlab.dhorman.cryptotrader.config

import io.netty.channel.ChannelOption
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import io.netty.handler.timeout.ReadTimeoutHandler
import io.netty.handler.timeout.WriteTimeoutHandler
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient
import org.springframework.web.reactive.socket.client.WebSocketClient
import reactor.netty.http.client.HttpClient
import reactor.netty.tcp.ProxyProvider
import java.util.concurrent.TimeUnit

@Configuration
class HttpClientConfig {
    private fun defaultHttpClient(
        connectTimeoutMs: Long = 5000,
        readTimeoutMs: Long = 5000,
        writeTimeoutMs: Long = 5000
    ): HttpClient {
        val sslContextBuilder = SslContextBuilder.forClient()

        if (System.getenv("HTTP_CERT_TRUST_ALL") != null) {
            sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE)
        }

        val proxyOptions = fun(opt: ProxyProvider.TypeSpec) {
            val tpe = when (System.getenv("HTTP_PROXY_TYPE")) {
                "http" -> ProxyProvider.Proxy.HTTP
                "socks5" -> ProxyProvider.Proxy.SOCKS5
                else -> throw RuntimeException("Can't recognize HTTP_PROXY_TYPE option")
            }

            val host = System.getenv("HTTP_PROXY_HOST")
                ?: throw Exception("Please define HTTP_PROXY_HOST env variable")

            val port = System.getenv("HTTP_PROXY_PORT")?.toInt()
                ?: throw Exception("Please define valid port number for HTTP_PROXY_PORT env variable")

            opt.type(tpe).host(host).port(port)
        }

        return HttpClient.create()
            .headers { it[HttpHeaderNames.USER_AGENT] = "trading-robot" }
            .secure { it.sslContext(sslContextBuilder.build()) }
            .tcpConfiguration { tcpClient ->
                var client = tcpClient
                client = client.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs.toInt())
                if (System.getenv("HTTP_PROXY_ENABLED") != null) client = client.proxy(proxyOptions)
                client = client.doOnConnected { conn ->
                    conn.addHandlerLast(ReadTimeoutHandler(readTimeoutMs, TimeUnit.MILLISECONDS))
                    conn.addHandlerLast(WriteTimeoutHandler(writeTimeoutMs, TimeUnit.MILLISECONDS))
                }
                client
            }
    }

    @Bean
    fun webClient(): WebClient {
        return WebClient.builder().clientConnector(ReactorClientHttpConnector(defaultHttpClient())).build()
    }

    @Bean
    fun websocketClient(): WebSocketClient {
        val webSocketClient = ReactorNettyWebSocketClient(defaultHttpClient(readTimeoutMs = 4000))
        webSocketClient.maxFramePayloadLength = 65536 * 4
        return webSocketClient
    }
}
