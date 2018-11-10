ThisBuild / scalaVersion := "2.12.7"
ThisBuild / version := "1.0.0-SNAPSHOT"
ThisBuild / organization := "com.gitlab.dhorman"

lazy val vertxVersion = "3.5.3"
lazy val circeVersion = "0.10.0"

lazy val `crypto-trader` = (project in file("."))
  .settings(
    name := "crypto-trader",
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) => MergeStrategy.last
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
      case PathList("codegen.json") => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    javacOptions ++= Seq("-source", "1.10", "-target", "1.10"),
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
    ),
    libraryDependencies ++= Seq(
      "io.vertx" %% "vertx-lang-scala" % vertxVersion,
      "io.vertx" %% "vertx-web-scala" % vertxVersion,
      "io.vertx" %% "vertx-web-client-scala" % vertxVersion,
      "io.vertx" %% "vertx-auth-jwt-scala" % vertxVersion,
      "io.vertx" %% "vertx-mysql-postgresql-client-scala" % vertxVersion,
      "io.vertx" %% "vertx-jdbc-client-scala" % vertxVersion,
      "io.vertx" %% "vertx-sql-common-scala" % vertxVersion,
      "io.vertx" % "vertx-web-client" % vertxVersion,
      "io.vertx" % "vertx-rx-java2" % vertxVersion,
      "io.vertx" % "vertx-reactive-streams" % vertxVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "io.circe" %% "circe-optics" % circeVersion,
      "io.circe" %% "circe-java8" % circeVersion,
      "io.circe" %% "circe-generic-extras" % circeVersion,
      "com.softwaremill.macwire" %% "macros" % "2.3.1",
      "com.softwaremill.common" %% "tagging" % "2.2.1",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "org.postgresql" % "postgresql" % "42.2.2",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "io.reactivex" %% "rxscala" % "0.26.5",
      "io.projectreactor" % "reactor-core" % "3.2.0.RELEASE",
      "io.projectreactor" %% "reactor-scala-extensions" % "0.3.5",
      "com.roundeights" %% "hasher" % "1.2.0",
    )
  )