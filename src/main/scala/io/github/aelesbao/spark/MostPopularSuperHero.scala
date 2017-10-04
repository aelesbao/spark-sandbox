package io.github.aelesbao.spark

import io.github.aelesbao.spark.data.MarvelDataSource
import org.apache.spark._

object MostPopularSuperHero {
  def parseNames(row: Array[String]): Option[(Int, String)] = {
    if (row.length > 1) Some(row(0).trim().toInt, row(1)) else None
  }

  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", getClass.getName)

    val mostPopular = new MarvelDataSource(sc, "marvel-graph")
      .map(row => (row(0).toInt, row.length - 1)) // Convert to (heroID, number of connections) RDD
      .reduceByKey((x, y) => x + y) // Combine entries that span more than one line
      .map(x => (x._2, x._1)) // Flip it to # of connections, hero ID
      .max() // Find the max # of connections

    // Look up the name (lookup returns an array of results, so we need to access the first result with (0)).
    val mostPopularName = new MarvelDataSource(sc, "marvel-names").rdd
      .flatMap(parseNames)
      .lookup(mostPopular._2)(0)

    println(
      s"$mostPopularName is the most popular superhero with ${mostPopular._1} co-appearances.")
  }
}
