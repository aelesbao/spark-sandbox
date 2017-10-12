package io.github.aelesbao.spark.data

import java.nio.charset.CodingErrorAction

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.Codec

object CsvDataSource extends Logging {
  // Handle character encoding issues
  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  def apply(dataSourcePath: String, mapper: String => Array[String])(
    implicit sc: SparkContext
  ): RDD[Array[String]] = {
    logger.debug(s"Loading data source '${dataSourcePath}' from resource path")
    sc.textFile(s"data/${dataSourcePath}").map(mapper)
  }

  def withHeader(dataSourcePath: String, mapper: String => Array[String])(
    implicit sc: SparkContext
  ): RDD[Map[String, String]] = {
    val rdd = apply(dataSourcePath, mapper)

    val headerLine = rdd.take(1)(0)
    val headerIndex = headerLine.zipWithIndex.toMap

    def header(array: Array[String], key: String): String =
      array(headerIndex(key))

    def toMap(row: Array[String]): Map[String, String] =
      headerIndex.keys
        .map(key => (key, header(row, key)))
        .toMap

    rdd
      .filter(header(_, headerIndex.keys.head) != headerIndex.keys.head)
      .map(toMap)
  }
}
