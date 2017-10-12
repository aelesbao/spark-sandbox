package io.github.aelesbao.spark.data

import org.apache.spark.SparkContext

object MovieLensDataSource {
  def apply(dataSourceName: String)(implicit sc: SparkContext) =
    CsvDataSource.withHeader(s"/ml-latest-small/${dataSourceName}.csv", mapper)

  private def mapper(line: String): Array[String] =
    line
      .split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")
      .map(cell => cell.replace("\"", "").trim)
}
