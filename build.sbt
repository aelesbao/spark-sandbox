import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "io.github.aelesbao.spark",
      scalaVersion := "2.11.11",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "spark-sandbox",
    libraryDependencies ++= Seq(sparkCore) ++ loggingDeps ++ testingDeps
  )
