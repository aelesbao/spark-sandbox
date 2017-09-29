package io.github.aelesbao.spark

import io.github.aelesbao.spark.data.MovieLensDataSource
import org.apache.spark.SparkContext

object PopularMovies {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", getClass.getName)

    val data = new MovieLensDataSource(sc, "ratings")
    val moviesRatings = data(row => (row("movieId"), 1))

    val movieCounts = moviesRatings.reduceByKey((x, y) => x + y)
    val flippedResults = movieCounts.map(x => (x._2, x._1))
    val sortedResults = flippedResults.sortByKey(false)

    sortedResults.take(10).foreach(println)
  }

}
