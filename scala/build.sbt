ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.0"

lazy val root = (project in file("."))
  .settings(
    name := "trial1",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-parser-combinators" % "2.0.0",
      "org.apache.spark" %% "spark-core" % "3.1.2",
      "org.json4s" %% "json4s-jackson" % "3.5.0",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.1",
      "org.scala-lang" % "scala-reflect" % scalaVersion.value
    )
  )