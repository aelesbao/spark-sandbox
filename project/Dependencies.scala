import sbt._

object Dependencies {
  val customResolvers = Seq(
    "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
  )

  private val sparkVersion: String = "2.4.0"
  val spark: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
  )

  val dataSources = Seq(
    "harsha2010" % "magellan" % "1.0.5-s_2.11",
    "org.postgresql" % "postgresql" % "42.1.4"
  )

  private val slf4jVersion = "1.7.25"
  val logging: Seq[ModuleID] = Seq(
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
  )

  val testing = Seq(
    "org.scalatest" %% "scalatest" % "3.0.4" % Test
  )
}
