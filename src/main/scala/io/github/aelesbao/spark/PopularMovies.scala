package io.github.aelesbao.spark

import org.apache.spark.sql.SparkSession

object PopularMovies extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName(getClass.getName)
    .getOrCreate()

  import spark.implicits._

  val ratings = loadDF("ratings")
  val ratingsPerMovie = ratings.groupBy("movieId").count().cache()
  ratingsPerMovie.printSchema()

  val movies = loadDF("movies").cache()
  movies.printSchema()

  val results = ratingsPerMovie
    .join(movies, "movieId")
    .orderBy($"count".desc)
    .cache()

  results.printSchema()

  println(f"   id | movie")
  results
    .map(row => f"${row.getAs("movieId")}%5d | ${row.getAs("title")}")
    .take(10)
    .foreach(println)

  private def loadDF(dataSet: String) = {
    spark.read
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .csv(s"data/ml-latest-small/${dataSet}.csv")
  }
}
