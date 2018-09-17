package com.griddynamics.analytics.importers

import com.griddynamics.analytics.CountryBlock
import org.apache.spark.sql.{Dataset, SparkSession}

object CountryBlockImporter {
  def apply(spark: SparkSession, eventsDirectory: String*): CountryBlockImporter =
    new CountryBlockImporter(spark, eventsDirectory: _*)
}

class CountryBlockImporter(spark: SparkSession, eventsDirectory: String*) {
  import spark.implicits._

  def importData(): Dataset[CountryBlock] = {
    spark.read
      .schema(CountryBlock.schema)
      .option("header", "true")
      .csv(eventsDirectory: _*)
      .as[CountryBlock]
  }
}
