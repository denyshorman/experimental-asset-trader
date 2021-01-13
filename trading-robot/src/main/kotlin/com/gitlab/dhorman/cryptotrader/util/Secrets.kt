package com.gitlab.dhorman.cryptotrader.util

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

interface ISecrets {
    fun get(secretId: String): String?
}

object Secrets : ISecrets {
    private val secrets = EnvBasedSecrets(DockerSecrets(NoSecrets()))

    override fun get(secretId: String): String? = secrets.get(secretId)
}

private class EnvBasedSecrets(private val secret: ISecrets) : ISecrets {
    override fun get(secretId: String): String? {
        return System.getenv(secretId) ?: secret.get(secretId)
    }
}

private abstract class FileBasedSecrets(private val secret: ISecrets) : ISecrets {
    protected abstract fun getPathPrefix(): String

    override fun get(secretId: String): String? {
        try {
            val filePath = Paths.get(getPathPrefix() + secretId)
            if (!Files.exists(filePath)) return secret.get(secretId)
            val contents = Files.readAllBytes(filePath)
            return String(contents, StandardCharsets.UTF_8)
        } catch (_: Exception) {
            return secret.get(secretId)
        }
    }
}

private class DockerSecrets(secret: ISecrets) : FileBasedSecrets(secret) {
    override fun getPathPrefix() = "/run/secrets/"
}

private class NoSecrets : ISecrets {
    override fun get(secretId: String): String? = null
}
