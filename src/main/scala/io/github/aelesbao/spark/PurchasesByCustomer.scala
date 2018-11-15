package io.github.aelesbao.spark

import com.typesafe.scalalogging.LazyLogging
import io.github.aelesbao.spark.data.CsvDataSource
import org.apache.spark.SparkContext

object PurchasesByCustomer extends App with LazyLogging {
  implicit val sc: SparkContext = new SparkContext("local[*]", getClass.getName)

  logger.info("Calculating purchases by customer")

  val results = CsvDataSource("customer-orders.csv", _.split(","))
    .map(row => (row(0).toInt, BigDecimal(row(2))))
    .reduceByKey((a, b) => a + b)
    .sortBy(_._2, ascending = false)
    .collect()

  results.foreach(println)
}
