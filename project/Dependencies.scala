import sbt._

object Dependencies {
  lazy val sparkVersion = "2.2.0"
  lazy val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion exclude("org.slf4j", "slf4j-log4j12")

  lazy val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
  lazy val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.4"

  lazy val loggingDeps = Seq(logback, scalaLogging)
  lazy val testingDeps = Seq(scalaTest % Test)
}
