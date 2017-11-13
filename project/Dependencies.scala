import sbt._

object Dependencies {
  val customResolvers = Seq(
    "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
  )

  val sparkVersion = "2.2.0"
  val sparkDeps = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion exclude("log4j", "log4j"),
    "org.apache.spark" %% "spark-sql" % sparkVersion
  )

  val dataSourceDependencies = Seq(
    "harsha2010" % "magellan" % "1.0.5-s_2.11",
    "org.postgresql" % "postgresql" % "42.1.4"
  )

  val log4jVersion = "2.9.1"
  val loggingDeps = Seq(
    "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
    "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
    "org.apache.logging.log4j" % "log4j-1.2-api" % log4jVersion,
    "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"
  )

  val testingDeps = Seq("org.scalatest" %% "scalatest" % "3.0.4" % Test)
}
