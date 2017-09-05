package io.github.aelesbao.spark

import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkContext

object RatingsCounter {
  val logger = Logger(getClass)

  def main(args: Array[String]) {
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the ratings data into an RDD
    val dataUrl = getClass().getResource("/ml-latest-small/ratings.csv")
    val csv = sc.textFile(dataUrl.getPath())
    val data = csv.map(line => line.split(","))

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userId, movieId, rating, timestamp)
    val header = new SimpleCSVHeader(data.take(1)(0))
    val rows = data.filter(line => header(line, "userId") != "userId") // filter the header out
    val ratings = rows.map(row => header(row, "rating"))

    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()

    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1)

    // Print each result on its own line.
    sortedResults.foreach(println)
  }
}

class SimpleCSVHeader(header: Array[String]) extends Serializable {
  val index = header.zipWithIndex.toMap

  def apply(array: Array[String], key: String): String = array(index(key))
}
