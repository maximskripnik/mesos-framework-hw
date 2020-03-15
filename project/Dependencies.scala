import sbt._

object Dependencies {
  val akkaHttp = "com.typesafe.akka" %% "akka-http" % "10.1.11"
  val akkaStreams = "com.typesafe.akka" %% "akka-stream" % "2.6.3"
  val akkaActorTyped = "com.typesafe.akka" %% "akka-actor-typed" % "2.6.3"
  val alpakkaSimpleCodecs = "com.lightbend.akka" %% "akka-stream-alpakka-simple-codecs" % "1.1.2"
  val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
  val mesos = "org.apache.mesos" % "mesos" % "1.9.0"
}
