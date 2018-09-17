package com.griddynamics.analytics.importers

import com.griddynamics.analytics.Event
import org.apache.spark.sql.{Dataset, SparkSession}

object EventsImporter {
  def apply(spark: SparkSession, eventsDirectory: String*): EventsImporter =
    new EventsImporter(spark, eventsDirectory: _*)
}

class EventsImporter(spark: SparkSession, eventsDirectory: String*) {
  import spark.implicits._

  def importData(): Dataset[Event] = {
    spark.read
      .schema(Event.schema)
      .csv(eventsDirectory: _*)
      .as[Event]
  }
}
