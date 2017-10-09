package io.github.aelesbao.spark.data

import java.nio.charset.CodingErrorAction

import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.Codec

object CsvDataSource {
  private val log = Logger(getClass)

  // Handle character encoding issues
  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  def apply(dataSourcePath: String)(
    implicit sc: SparkContext,
    parseCsvLine: String => Array[String]
  ): RDD[Array[String]] = {
    log.debug(s"Loading data source '${dataSourcePath}' from resource path")
    sc.textFile(s"data/${dataSourcePath}").map(parseCsvLine)
  }

  def withHeader(dataSourcePath: String)(
    implicit sc: SparkContext,
    parseCsvLine: String => Array[String]
  ): RDD[Map[String, String]] = {
    val rdd = apply(dataSourcePath)

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
