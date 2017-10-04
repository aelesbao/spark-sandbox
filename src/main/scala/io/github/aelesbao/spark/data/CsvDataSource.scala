package io.github.aelesbao.spark.data

import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkContext

object CsvDataSource {
  private val log = Logger(getClass)

  def apply(dataSourcePath: String)(
    implicit sc: SparkContext,
    parseCsvLine: String => Array[String]
  ) = {
    log.debug(s"Loading data source '${dataSourcePath}' from resource path")

    val resource = getClass().getResource(dataSourcePath)
    if (resource == null) {
      throw new IllegalArgumentException(
        s"Could not find a data source on '${dataSourcePath}'")
    }

    sc.textFile(resource.getPath()).map(parseCsvLine)
  }
}

object MovieLensDataSource {
  implicit private def parseCsvLine(line: String): Array[String] =
    line
      .split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")
      .map(cell => cell.replace("\"", "").trim)

  def apply(dataSourceName: String)(implicit sc: SparkContext) = {
    val rdd = CsvDataSource(s"/ml-latest-small/${dataSourceName}.csv")

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

object MarvelDataSource {
  implicit private def parseCsvLine(line: String): Array[String] =
    line
      .split("\\s+(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")
      .map(cell => cell.replace("\"", "").trim)

  def apply(dataSourceName: String)(implicit sc: SparkContext) =
    CsvDataSource(s"/marvel/${dataSourceName}.txt")
}
