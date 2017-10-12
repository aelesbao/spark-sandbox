package io.github.aelesbao.spark.apps

import io.github.aelesbao.spark.data.CsvDataSource
import org.apache.spark.sql._

object DataFrames extends App {

  // Use new SparkSession interface in Spark 2.0
  val spark = SparkSession.builder
    .appName(getClass.getName)
    .master("local[*]")
    .getOrCreate()

  implicit val sc = spark.sparkContext

  import spark.implicits._

  val people = CsvDataSource("fakefriends.csv", _.split(','))
    .map(fields => Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt))
    .toDS()
    .cache()

  // There are lots of other ways to make a DataFrame.
  // For example, spark.read.json("json file path")
  // or sqlContext.table("Hive table name")

  println("Here is our inferred schema:")
  people.printSchema()

  println("Let's select the name column:")
  people.select("name").show()

  println("Filter out anyone over 21:")
  people.filter(people("age") < 21).show()

  println("Group by age:")
  people.groupBy("age").count().show()

  println("Make everyone 10 years older:")
  people.select(people("name"), people("age") + 10).show()

  spark.stop()

  case class Person(ID: Int, name: String, age: Int, numFriends: Int)
}
