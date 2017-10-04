package io.github.aelesbao.spark.data

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class MarvelDataSource(sc: SparkContext, dataSourceName: String)
    extends CsvDataSource(sc, s"/marvel/${dataSourceName}.txt") {

  def map[R: ClassTag](f: Array[String] => R): RDD[R] =
    rdd.map(f.apply(_))

  override protected def parseCsvLine(line: String): Array[String] =
    line
      .split("\\s+(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")
      .map(cell => cell.replace("\"", "").trim)
}
