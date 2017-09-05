package io.github.aelesbao.spark

import org.apache.spark.SparkContext

object PurchasesByCustomer {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "RatingsCounter")

    val dataUrl = getClass().getResource("/customer-orders.csv")
    val csv = sc.textFile(dataUrl.getPath())
    val rows = csv.map(line => line.split(","))

    val results = rows
      .map(row => (row(0).toInt, BigDecimal(row(2))))
      .reduceByKey((a, b) => a + b)
      .sortBy(_._2, false)
      .collect()

    results.foreach(println)
  }
}
