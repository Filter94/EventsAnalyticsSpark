package com.griddynamics.analytics.importers

import com.griddynamics.analytics.SparkContextKeeper.spark
import spark.implicits._
import com.griddynamics.analytics.CountryLocation
import org.apache.spark.sql.Dataset

object CountryLocationsImporter {
  def apply(eventsDirectory: String*): CountryLocationsImporter = new CountryLocationsImporter(eventsDirectory: _*)
}

class CountryLocationsImporter(eventsDirectory: String*) {
  def importData(): Dataset[CountryLocation] = {
    spark.read
      .schema(CountryLocation.schema)
      .option("header", "true")
      .csv(eventsDirectory: _*)
      .as[CountryLocation]
  }
}
