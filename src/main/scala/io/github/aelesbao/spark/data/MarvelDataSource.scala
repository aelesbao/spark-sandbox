package io.github.aelesbao.spark.data

import org.apache.spark.SparkContext

object MarvelDataSource {
  implicit private def parseCsvLine(line: String): Array[String] =
    line
      .split("\\s+(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")
      .map(cell => cell.replace("\"", "").trim)

  def apply(dataSourceName: String)(implicit sc: SparkContext) =
    CsvDataSource(s"/marvel/${dataSourceName}.txt")
}
