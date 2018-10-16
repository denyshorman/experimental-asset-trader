package com.gitlab.dhorman.cryptotrader.util

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

trait Secrets {
  def get(secretId: String): Option[String]
}

object Secrets extends Secrets {
  private val secrets = new NoSecrets with DockerSecrets with EnvBasedSecrets

  override def get(secretId: String): Option[String] = secrets.get(secretId)


  private trait EnvBasedSecrets extends Secrets {
    abstract override def get(secretId: String): Option[String] = {
      Option(System.getenv(secretId)).orElse({super.get(secretId)})
    }
  }

  private trait FileBasedSecrets extends Secrets {
    protected def getPathPrefix: String

    abstract override def get(secretId: String): Option[String] = try {
      val filePath = Paths.get(getPathPrefix + secretId)
      if (!Files.exists(filePath)) return super.get(secretId)
      val contents = Files.readAllBytes(filePath)
      Some(new String(contents, StandardCharsets.UTF_8))
    } catch {
      case _: Exception => super.get(secretId)
    }
  }

  private trait DockerSecrets extends FileBasedSecrets {
    override protected def getPathPrefix = "/run/secrets/"
  }

  private class NoSecrets extends Secrets {
    override def get(secretId: String): Option[String] = None
  }
}
