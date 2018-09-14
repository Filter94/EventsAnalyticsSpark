package com.griddynamics.analytics

import com.griddynamics.analytics.importers.{CountryBlockImporter, CountryLocationsImporter, EventsImporter}
import org.apache.spark.sql.{Dataset, SaveMode}

object Main extends App {
  def loadDsToDbTable[T](ds: Dataset[T], dbConfig: DbConnectionConfiguration, tableName: String): Unit = {
    ds.write
      .mode(SaveMode.Overwrite)
      .jdbc(dbConfig.jdbcUrl, tableName, dbConfig.properties)
  }

  if (args.length == 4) {
    val events = EventsImporter(args(0).split(","): _*).importData()
    val countryBlocks = CountryBlockImporter(args(1).split(","): _*).importData()
    val countryLocations = CountryLocationsImporter(args(2).split(","): _*).importData()
    val dbConnectionConfiguration = DbConnectionConfiguration(args(3))
    val eventsAnalytics = DsEventsAnalytics(events)
    val geodataAnalytics = DsEventsAndGeoDataAnalytics(events, countryBlocks, countryLocations)
    loadDsToDbTable(eventsAnalytics.top10Categories(), dbConnectionConfiguration, "top_categories")
    loadDsToDbTable(eventsAnalytics.top10ByCategory(), dbConnectionConfiguration, "top_10_by_category")
    loadDsToDbTable(geodataAnalytics.top10CountriesBySells(), dbConnectionConfiguration, "top_10_countries")
  } else {
    "4 parameters needed. First on is events path, second one is country blocks path," +
      " third one is country locations path, fourth one is db.properties path."
  }
}
