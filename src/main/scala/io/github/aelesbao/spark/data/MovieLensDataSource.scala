package io.github.aelesbao.spark.data

import org.apache.spark.SparkContext

object MovieLensDataSource {
  implicit private def parseCsvLine(line: String): Array[String] =
    line
      .split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")
      .map(cell => cell.replace("\"", "").trim)

  def apply(dataSourceName: String)(implicit sc: SparkContext) =
    CsvDataSource
        .withHeader(s"/ml-latest-small/${dataSourceName}.csv")
}
