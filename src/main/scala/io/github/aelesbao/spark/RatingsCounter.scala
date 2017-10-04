package io.github.aelesbao.spark

import io.github.aelesbao.spark.data.MovieLensDataSource
import org.apache.spark.SparkContext

object RatingsCounter {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", getClass.getName)

    val data = new MovieLensDataSource(sc, "ratings")
    val ratings = data.map(_("rating"))

    val sortedResults = ratings
      .countByValue()
      .toSeq
      .sortBy(_._1)

    sortedResults.foreach(println)
  }
}
