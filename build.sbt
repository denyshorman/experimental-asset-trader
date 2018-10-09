ThisBuild / scalaVersion := "2.12.7"
ThisBuild / version := "1.0.0-SNAPSHOT"
ThisBuild / organization := "com.gitlab.dhorman"

lazy val vertxVersion = "3.5.3"
lazy val circeVersion = "0.9.3"

lazy val `crypto-trader` = (project in file("."))
  .settings(
    name := "crypto-trader",
    javacOptions ++= Seq("-source", "1.10", "-target", "1.10"),

    libraryDependencies ++= Seq(
      "io.vertx" %% "vertx-lang-scala" % vertxVersion,
      "io.vertx" %% "vertx-web-scala" % vertxVersion,
      "io.vertx" %% "vertx-auth-jwt-scala" % vertxVersion,
      "io.vertx" %% "vertx-mysql-postgresql-client-scala" % vertxVersion,
      "io.vertx" %% "vertx-jdbc-client-scala" % vertxVersion,
      "io.vertx" %% "vertx-sql-common-scala" % vertxVersion,
      "io.vertx" % "vertx-rx-java2" % vertxVersion,
      "io.vertx" % "vertx-reactive-streams" % vertxVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "com.softwaremill.macwire" %% "macros" % "2.3.1",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "org.postgresql" % "postgresql" % "42.2.2",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "io.reactivex" %% "rxscala" % "0.26.5",
      "io.projectreactor" % "reactor-core" % "3.2.0.RELEASE",
    )
  )