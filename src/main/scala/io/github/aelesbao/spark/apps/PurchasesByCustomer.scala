package io.github.aelesbao.spark.apps

import io.github.aelesbao.spark.data.CsvDataSource
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.SparkContext

object PurchasesByCustomer extends App with Logging {
  implicit val sc = new SparkContext("local[*]", getClass.getName)

  logger.info("Calculating purchases by customer")

  val results = CsvDataSource("customer-orders.csv", _.split(","))
    .map(row => (row(0).toInt, BigDecimal(row(2))))
    .reduceByKey((a, b) => a + b)
    .sortBy(_._2, false)
    .collect()

  results.foreach(println)
}
