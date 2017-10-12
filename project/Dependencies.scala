import sbt._

object Dependencies {
  lazy val sparkVersion = "2.2.0"
  lazy val sparkDeps = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided" exclude("log4j", "log4j"),
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  )

  lazy val log4jVersion = "2.9.1"
  lazy val loggingDeps = Seq(
    "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
    "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
    "org.apache.logging.log4j" % "log4j-1.2-api" % log4jVersion,
    "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"
  )

  lazy val testingDeps = Seq("org.scalatest" %% "scalatest" % "3.0.4" % Test)
}
