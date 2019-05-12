package com.gitlab.dhorman.cryptotrader.config

import com.gitlab.dhorman.cryptotrader.util.Secrets
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.r2dbc.function.DatabaseClient
import org.springframework.data.r2dbc.function.TransactionalDatabaseClient

@Configuration
class PostgresConfig {
    @Bean("pg_conn_factory")
    fun getPostgresqlConnectionFactory(): PostgresqlConnectionFactory {
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

        return PostgresqlConnectionFactory(
            PostgresqlConnectionConfiguration.builder()
                .host(host)
                .port(port)
                .database(database)
                .username(login)
                .password(password)
                .build()
        )
    }

    @Bean("pg_client")
    fun getDatabaseClient(@Qualifier("pg_conn_factory") connectionFactory: PostgresqlConnectionFactory): DatabaseClient {
        return DatabaseClient.create(connectionFactory)
    }

    @Bean("pg_tran_client")
    fun getTransactionalDatabaseClient(@Qualifier("pg_conn_factory") connectionFactory: PostgresqlConnectionFactory): TransactionalDatabaseClient {
        return TransactionalDatabaseClient.create(connectionFactory)
    }
}