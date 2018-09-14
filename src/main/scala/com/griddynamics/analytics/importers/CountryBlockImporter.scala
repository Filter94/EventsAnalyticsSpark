package com.griddynamics.analytics.importers

import com.griddynamics.analytics.SparkContextKeeper.spark
import spark.implicits._
import com.griddynamics.analytics.CountryBlock
import org.apache.spark.sql.Dataset

object CountryBlockImporter {
  def apply(eventsDirectory: String*): CountryBlockImporter = new CountryBlockImporter(eventsDirectory: _*)
}

class CountryBlockImporter(eventsDirectory: String*) {
  def importData(): Dataset[CountryBlock] = {
    spark.read
      .schema(CountryBlock.schema)
      .option("header", "true")
      .csv(eventsDirectory: _*)
      .as[CountryBlock]
  }
}
