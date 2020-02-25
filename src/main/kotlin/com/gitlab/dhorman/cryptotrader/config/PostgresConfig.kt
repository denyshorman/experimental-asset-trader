package com.gitlab.dhorman.cryptotrader.config

import com.gitlab.dhorman.cryptotrader.util.Secrets
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions.*
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration
import org.springframework.data.r2dbc.connectionfactory.R2dbcTransactionManager
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.transaction.ReactiveTransactionManager

@Configuration
class PostgresConfig : AbstractR2dbcConfiguration() {
    @Bean("pg_client")
    fun databaseClient(): DatabaseClient {
        return DatabaseClient.create(connectionFactory())
    }

    @Bean("pg_tran_manager")
    fun transactionManager(): ReactiveTransactionManager {
        return R2dbcTransactionManager(connectionFactory())
    }

    @Bean("pg_conn_factory")
    override fun connectionFactory(): ConnectionFactory {
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

        val options = builder()
            .option(HOST, host)
            .option(PORT, port)
            .option(USER, login)
            .option(PASSWORD, password)
            .option(DATABASE, database)
            .option(DRIVER, "postgresql")
            .build()

        return ConnectionFactories.get(options)
    }
}
