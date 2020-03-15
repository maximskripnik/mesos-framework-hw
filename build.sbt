import Dependencies._

ThisBuild / scalaVersion := "2.13.1"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.newflayer"
ThisBuild / organizationName := "newflayer"

lazy val root = (project in file("."))
  .settings(
    name := "mesos-hw-framework",
    libraryDependencies ++= List(
      akkaHttp,
      akkaStreams,
      akkaActorTyped,
      alpakkaSimpleCodecs,
      mesos,
      logback
    ),
    scalacOptions += "-Ywarn-unused"
  )
