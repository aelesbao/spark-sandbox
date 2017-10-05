package io.github.aelesbao.spark.apps.movielens

import io.github.aelesbao.spark.data.MovieLensDataSource
import org.apache.spark.SparkContext

object RatingsCounter {
  def main(args: Array[String]) {
    implicit val sc = new SparkContext("local[*]", getClass.getName)

    val data = MovieLensDataSource("ratings")
    val ratings = data.map(_("rating"))

    val sortedResults = ratings
      .countByValue()
      .toSeq
      .sortBy(_._1)

    sortedResults.foreach(println)
  }
}
