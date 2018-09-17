package com.griddynamics.analytics.importers

import com.griddynamics.analytics.CountryLocation
import org.apache.spark.sql.{Dataset, SparkSession}

object CountryLocationsImporter {
  def apply(spark: SparkSession, eventsDirectory: String*): CountryLocationsImporter =
    new CountryLocationsImporter(spark, eventsDirectory: _*)
}

class CountryLocationsImporter(spark: SparkSession, eventsDirectory: String*) {
  import spark.implicits._

  def importData(): Dataset[CountryLocation] = {
    spark.read
      .schema(CountryLocation.schema)
      .option("header", "true")
      .csv(eventsDirectory: _*)
      .as[CountryLocation]
  }
}
