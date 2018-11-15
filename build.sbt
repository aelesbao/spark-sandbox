lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "io.github.aelesbao.spark",
      scalaVersion := "2.11.11",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "spark-sandbox",
    resolvers ++= Dependencies.customResolvers,
    libraryDependencies ++= Dependencies.spark ++
      Dependencies.dataSources ++
      Dependencies.logging ++
      Dependencies.testing
  )
