package io.github.aelesbao.spark.data

import org.apache.spark.SparkContext

object MarvelDataSource {
  def apply(dataSourceName: String)(implicit sc: SparkContext) =
    CsvDataSource(s"/marvel/${dataSourceName}.txt", mapper)

  private def mapper(line: String): Array[String] =
    line
      .split("\\s+(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")
      .map(cell => cell.replace("\"", "").trim)
}
