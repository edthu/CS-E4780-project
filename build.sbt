ThisBuild / scalaVersion := "3.3.4"
ThisBuild / version := "0.1.0-SNAPSHOT"

import sbtassembly.AssemblyPlugin.autoImport.*
import sbtassembly.MergeStrategy

lazy val root = project
  .in(file("."))
  .settings(
    name := "cs-e4780-ingestion",
    Compile / scalaSource := baseDirectory.value / "src" / "ingestion" / "main" / "scala",
    Test / scalaSource := baseDirectory.value / "src" / "ingestion" / "test" / "scala",
    libraryDependencies ++= Seq(
      "org.apache.commons" % "commons-csv" % "1.12.0",
      "com.lihaoyi" %% "ujson" % "4.1.0",
      "org.scalameta" %% "munit" % "1.0.3" % Test
    ),
    assembly / assemblyMergeStrategy := {
      case "META-INF/versions/9/module-info.class" => MergeStrategy.discard
      case path => (assembly / assemblyMergeStrategy).value(path)
    },
    Compile / run / fork := true,
    Compile / run / connectInput := true,
    Test / fork := true
  )