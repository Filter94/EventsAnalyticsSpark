package com.griddynamics.analytics

trait TestEventsImporter {
  protected val importer: EventsImporter = {
    val testDataPath: String = getClass.getResource("/events").getPath
    EventsImporter(testDataPath)
  }
}
