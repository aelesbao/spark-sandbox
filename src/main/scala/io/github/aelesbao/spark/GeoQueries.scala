package io.github.aelesbao.spark

import java.io.PrintWriter
import java.nio.file.{Files, Paths}

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession

object GeoQueries extends App with Logging {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName(getClass.getName)
    .getOrCreate()

  val path = "data/zones.geojson"
  writeZonesGeoJSON(path)

  val zones = spark.sqlContext.read
    .format("magellan")
    .option("type", "geojson")
    .load(path)

  logger.info(s"Total zones loaded: ${zones.count()}")

  zones.show()

  private def writeZonesGeoJSON(path: String) = {
    if (!Files.exists(Paths.get(path))) {
      val zonesGeoJSON = loadZonesFromDB()
      new PrintWriter(path) {
        write(zonesGeoJSON)
        close()
      }
    }
  }

  private def loadZonesFromDB(): String = {
    logger.info("Loading zones from database")

    val jdbcUrl = sys.env("DB_URL")
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", sys.env("DB_USER"))
    connectionProperties.put("password", sys.env("DB_PASSWORD"))

    val query =
      """
        |select jsonb_build_object(
        |  'type',     'FeatureCollection',
        |  'features', jsonb_agg(feature)
        |) as feature_collection
        |from (
        |  select jsonb_build_object(
        |    'type',       'Feature',
        |    'id',         id,
        |    'properties', to_jsonb(zones) - 'id' - 'geo_json',
        |    'geometry',   postgis.ST_AsGeoJSON(geo_json::postgis.geometry)::jsonb
        |  ) as feature
        |  from zones
        |) as features;
      """.stripMargin

    import java.sql.DriverManager
    val connection = DriverManager.getConnection(jdbcUrl, connectionProperties)
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(query)
    if (!resultSet.next()) throw new RuntimeException("Query didn't return any result")
    val featureCollection = resultSet.getString("feature_collection")
    connection.close()

    featureCollection
  }
}
