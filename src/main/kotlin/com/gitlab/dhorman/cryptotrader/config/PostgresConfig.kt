package com.gitlab.dhorman.cryptotrader.config

import com.gitlab.dhorman.cryptotrader.util.Secrets
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.r2dbc.function.DatabaseClient

@Configuration
class PostgresConfig {
    @Bean("postgres")
    fun getDatabaseClient(): DatabaseClient {
        val host = Secrets.get("POSTGRES_HOST")
            ?: throw RuntimeException("Please define POSTGRES_HOST environment variable")

        val port = Secrets.get("POSTGRES_PORT")?.toIntOrNull()
            ?: throw RuntimeException("Please define POSTGRES_PORT environment variable")

        val login = Secrets.get("POSTGRES_USER")
            ?: throw RuntimeException("Please define POSTGRES_USER environment variable")

        val password = Secrets.get("POSTGRES_PASSWORD")
            ?: throw RuntimeException("Please define POSTGRES_PASSWORD environment variable")

        val database = Secrets.get("POSTGRES_DB")
            ?: throw RuntimeException("Please define POSTGRES_DB environment variable")

        val connectionFactory = PostgresqlConnectionFactory(
            PostgresqlConnectionConfiguration.builder()
                .host(host)
                .port(port)
                .database(database)
                .username(login)
                .password(password)
                .build()
        )

        return DatabaseClient.create(connectionFactory)
    }
}