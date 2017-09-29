package io.github.aelesbao.spark.data

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class MovieLensDataSource(sc: SparkContext, dataSourceName: String) extends Serializable {

  private val resourcePath = getClass()
    .getResource(s"/ml-latest-small/${dataSourceName}.csv")
    .getPath()

  val csv = sc.textFile(resourcePath)
    .map(line => line.split(","))

  private val headerLine = csv.take(1)(0)
  private val headerIndex = headerLine.zipWithIndex.toMap

  private val data = csv.filter(header(_, headerIndex.keys.head) != headerIndex.keys.head)

  private def header(array: Array[String], key: String): String =
    array(headerIndex(key))

  def apply[R: ClassTag](f: ((String) => String) => R): RDD[R] =
    data.map(row => f.apply(header(row, _: String)))
}
