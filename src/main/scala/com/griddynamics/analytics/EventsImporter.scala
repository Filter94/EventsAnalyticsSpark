package com.griddynamics.analytics

import SparkContextKeeper.spark
import org.apache.spark.sql.Dataset
import spark.implicits._

object EventsImporter {
  def apply(eventsDirectory: String): EventsImporter = new EventsImporter(eventsDirectory)
}

class EventsImporter(eventsDirectory: String) {
  def importData(): Dataset[Event] = {
    spark.read
      .schema(Event.schema)
      .csv(eventsDirectory)
      .as[Event]
  }
}
