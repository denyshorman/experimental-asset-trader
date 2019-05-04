package com.gitlab.dhorman.cryptotrader.config

import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient
import org.springframework.web.reactive.socket.client.WebSocketClient
import reactor.netty.http.client.HttpClient
import reactor.netty.tcp.ProxyProvider


@Configuration
class HttpClientConfig {
    private fun defaultHttpClient(): HttpClient {
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
            .secure { it.sslContext(sslContextBuilder.build()) }
            .tcpConfiguration { tcpClient ->
                if (System.getenv("HTTP_PROXY_ENABLED") != null) {
                    tcpClient.proxy(proxyOptions)
                } else {
                    tcpClient
                }
            }
    }

    @Bean
    fun webClient(): WebClient {
        return WebClient.builder().clientConnector(ReactorClientHttpConnector(defaultHttpClient())).build()
    }

    @Bean
    fun websocketClient(): WebSocketClient {
        return ReactorNettyWebSocketClient(defaultHttpClient())
    }
}
