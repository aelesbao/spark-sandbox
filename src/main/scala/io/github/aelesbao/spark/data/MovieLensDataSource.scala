package io.github.aelesbao.spark.data

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class MovieLensDataSource(sc: SparkContext, dataSourceName: String)
    extends CsvDataSource(sc, s"/ml-latest-small/${dataSourceName}.csv") {

  private val headerLine = rdd.take(1)(0)
  private val headerIndex = headerLine.zipWithIndex.toMap

  private val data =
    rdd.filter(header(_, headerIndex.keys.head) != headerIndex.keys.head)

  private def header(array: Array[String], key: String): String =
    array(headerIndex(key))

  def map[R: ClassTag](f: ((String) => String) => R): RDD[R] =
    data.map(row => f.apply(header(row, _: String)))

  override protected def parseCsvLine(line: String) =
    line
      .split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")
      .map(cell => cell.replace("\"", ""))
}
