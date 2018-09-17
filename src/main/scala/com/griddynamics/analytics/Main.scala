package com.griddynamics.analytics

import com.griddynamics.analytics.exporters.DbExporter
import com.griddynamics.analytics.importers.{CountryBlockImporter, CountryLocationsImporter, EventsImporter}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Main extends App {
  val ARGS_NUM = 5
  if (args.length == ARGS_NUM) {
    val (sparkMaster, eventsFiles, countryBlockFiles, countryLocationsFiles, dbConf) =
      (args(0), args(1), args(2), args(3), args(4))
    val spark = SparkSession.builder()
      .appName("Geodata analytics")
      .config("spark.master", sparkMaster)
      .getOrCreate()
    val events = EventsImporter(spark, eventsFiles.split(","): _*).importData()
    val countryBlocks = CountryBlockImporter(spark, countryBlockFiles.split(","): _*).importData()
    val countryLocations = CountryLocationsImporter(spark, countryLocationsFiles.split(","): _*).importData()

    val eventsAnalytics = DsEventsAnalytics(spark, events)
    val geodataAnalytics = DsEventsAndGeoDataAnalytics(spark, events, countryBlocks, countryLocations)
    val dbExporter = DbExporter(DbConnectionConfiguration(dbConf))

    dbExporter.exportDsToDbTable(eventsAnalytics.top10Categories(),
      "top_categories", SaveMode.Overwrite)
    dbExporter.exportDsToDbTable(eventsAnalytics.top10ByCategory(),
      "top_10_by_category", SaveMode.Overwrite)
    dbExporter.exportDsToDbTable(geodataAnalytics.top10CountriesBySells(),
      "top_10_countries", SaveMode.Overwrite)
  } else {
    """
      |5 parameters needed:
      |First one is spark master string,
      |second on is events path,
      |third one is country blocks path,
      |fourth one is country locations path,
      |fifth one is db.properties path.""".stripMargin
  }
}
