package io.github.aelesbao.spark

import io.github.aelesbao.spark.data.CsvDataSource
import org.apache.spark.SparkContext

object PurchasesByCustomer {
  def main(args: Array[String]) {
    implicit val sc = new SparkContext("local[*]", "RatingsCounter")
    implicit def toCsvLine(line: String) = line.split(",")

    val results = CsvDataSource("/customer-orders.csv")
      .map(row => (row(0).toInt, BigDecimal(row(2))))
      .reduceByKey((a, b) => a + b)
      .sortBy(_._2, false)
      .collect()

    results.foreach(println)
  }
}
