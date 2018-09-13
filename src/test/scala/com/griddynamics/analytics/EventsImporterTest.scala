package com.griddynamics.analytics

import java.sql.Timestamp

import org.scalatest.FunSuite

class EventsImporterTest extends FunSuite with TrivialEventsImporter {
  test("All events are imported") {
    assert(eventsImporter.importData().count() == 3)
  }

  test("Events imported correctly.") {
    val expectedEvent = Event("product name 1", 123.123f, Timestamp.valueOf("2018-09-11 12:00:00.123"),
      "category 1", "1.0.0.128")
    assert(eventsImporter.importData().collect()(0) == expectedEvent)
  }
}
