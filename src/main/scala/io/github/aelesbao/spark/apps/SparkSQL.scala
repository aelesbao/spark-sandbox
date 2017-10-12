package io.github.aelesbao.spark.apps

import io.github.aelesbao.spark.data.CsvDataSource
import org.apache.spark.sql._

object SparkSQL extends App {

  // Use new SparkSession interface in Spark 2.0
  val spark = SparkSession.builder
    .appName(getClass.getName)
    .master("local[*]")
    .getOrCreate()

  implicit val sc = spark.sparkContext

  val people = CsvDataSource("fakefriends.csv", _.split(','))
    .map(fields => Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt))

  // Infer the schema, and register the DataSet as a table.
  import spark.implicits._
  val schemaPeople = people.toDS

  schemaPeople.printSchema()

  schemaPeople.createOrReplaceTempView("people")

  // SQL can be run over DataFrames that have been registered as a table
  val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

  val results = teenagers.collect()

  results.foreach(println)

  spark.stop()

  case class Person(ID: Int, name: String, age: Int, numFriends: Int)

}
