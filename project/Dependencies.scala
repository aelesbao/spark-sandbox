import sbt._

object Dependencies {
  lazy val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
  lazy val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.4"

  lazy val loggingDeps = Seq(logback, scalaLogging)
  lazy val testingDeps = Seq(scalaTest % Test)
}
