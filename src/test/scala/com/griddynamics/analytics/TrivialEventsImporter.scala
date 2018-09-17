package com.griddynamics.analytics

import com.griddynamics.analytics.importers.EventsImporter

trait TrivialEventsImporter extends LocalSparkContext {
  protected val eventsImporter: EventsImporter = {
    val testDataPath: String = getClass.getResource("/events/trivial.csv").getPath
    EventsImporter(spark, testDataPath)
  }
}
