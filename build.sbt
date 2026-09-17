ThisBuild / scalaVersion := "3.3.4"
ThisBuild / version := "0.1.0-SNAPSHOT"

import sbtassembly.AssemblyPlugin.autoImport.*
import sbtassembly.MergeStrategy

val kafkaVersion = "3.8.1"
val ujsonVersion = "4.1.0"
val commonsCsvVersion = "1.12.0"
val munitVersion = "1.0.3"
// Kafka 3.8 pulls slf4j-api 1.7.x; bind a no-op backend so `run` doesn't print
// the "Failed to load StaticLoggerBinder" warnings. Swap for slf4j-simple to
// see Kafka's own logs.
val slf4jNop = "org.slf4j" % "slf4j-nop" % "1.7.36"

// Total function (final catch-all), so no recursion into the task's own value.
// `first` merges duplicate .class files shipped by overlapping Kafka jars.
val appMergeStrategy: String => MergeStrategy = {
  case p if p.endsWith("module-info.class")    => MergeStrategy.discard
  case p if p.startsWith("META-INF/services/") => MergeStrategy.concat
  case p if p.startsWith("META-INF/")          => MergeStrategy.first
  case _                                       => MergeStrategy.first
}

lazy val commonSettings = Seq(
  Compile / scalaSource := baseDirectory.value / "main" / "scala",
  Test / scalaSource := baseDirectory.value / "test" / "scala",
  Compile / run / fork := true,
  Compile / run / connectInput := true,
  // Each module's base is src/<module>, so the forked run would otherwise use
  // that as its working dir. Anchor it to the repo root so relative paths like
  // `output/events.ndjson` resolve from where you launch sbt.
  Compile / run / baseDirectory := (ThisBuild / baseDirectory).value,
  Test / fork := true
)

lazy val assemblySettings = Seq(
  assembly / assemblyJarName := s"${name.value}.jar",
  assembly / assemblyMergeStrategy := appMergeStrategy
)

lazy val common = project
  .in(file("src/common"))
  .settings(commonSettings)
  .settings(
    name := "common",
    libraryDependencies += "com.lihaoyi" %% "ujson" % ujsonVersion
  )

lazy val ingestion = project
  .in(file("src/ingestion"))
  .dependsOn(common)
  .settings(commonSettings, assemblySettings)
  .settings(
    name := "ingestion",
    libraryDependencies ++= Seq(
      "org.apache.commons" % "commons-csv" % commonsCsvVersion,
      "com.lihaoyi" %% "ujson" % ujsonVersion,
      "org.scalameta" %% "munit" % munitVersion % Test
    )
  )

lazy val producer = project
  .in(file("src/producer"))
  .dependsOn(common)
  .settings(commonSettings, assemblySettings)
  .settings(
    name := "producer",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      "com.lihaoyi" %% "ujson" % ujsonVersion,
      slf4jNop,
      "org.scalameta" %% "munit" % munitVersion % Test
    )
  )

lazy val consumer = project
  .in(file("src/consumer"))
  .dependsOn(common)
  .settings(commonSettings, assemblySettings)
  .settings(
    name := "consumer",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      slf4jNop,
      "org.scalameta" %% "munit" % munitVersion % Test
    )
  )

lazy val streams = project
  .in(file("src/streams"))
  .dependsOn(common)
  .settings(commonSettings, assemblySettings)
  .settings(
    name := "streams",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-streams" % kafkaVersion,
      "com.lihaoyi" %% "ujson" % ujsonVersion,
      slf4jNop,
      "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % Test,
      "org.scalameta" %% "munit" % munitVersion % Test
    )
  )

lazy val root = project
  .in(file("."))
  .aggregate(common, ingestion, producer, consumer, streams)
  .settings(name := "cs-e4780")
