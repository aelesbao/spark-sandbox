package io.github.aelesbao.spark

import io.github.aelesbao.spark.data.MovieLensDataSource
import org.apache.spark.SparkContext

object PopularMovies {
  def main(args: Array[String]) {
    implicit val sc = new SparkContext("local[*]", getClass.getName)

    val ratingsPerMovie = MovieLensDataSource("ratings")
      .map(row => (row("movieId"), 1))
      .reduceByKey((x, y) => x + y)

    val movies = MovieLensDataSource("movies")
      .map(row => (row("movieId"), row("title")))

    val results = ratingsPerMovie
      .join(movies)
      .map(_._2)
      .sortBy(x => x._1, false)
      .map(x => f"${x._1}%5d | ${x._2}")

    results.take(10).foreach(println)
  }

}
