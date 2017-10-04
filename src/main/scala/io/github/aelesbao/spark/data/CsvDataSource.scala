package io.github.aelesbao.spark.data

import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkContext

abstract class CsvDataSource(
    sc: SparkContext,
    dataSourcePath: String
) extends Serializable {

  private val log = Logger(getClass)

  log.debug(s"Loading data source '${dataSourcePath}' from resource path")
  private val resource = getClass().getResource(dataSourcePath)
  if (resource == null) {
    throw new IllegalArgumentException(
      s"Could not find a data source on '${dataSourcePath}'")
  }

  val rdd = sc.textFile(resource.getPath()).map(parseCsvLine)

  protected def parseCsvLine(line: String): Array[String]
}
