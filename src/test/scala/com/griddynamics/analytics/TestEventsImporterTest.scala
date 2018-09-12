package com.griddynamics.analytics

import java.sql.Timestamp

import org.scalatest.FunSuite

class TestEventsImporterTest extends FunSuite with TestEventsImporter {
  test("All events are imported") {
    assert(importer.importData().count() == 3)
  }

  test("Events imported correctly.") {
    val expectedEvent = Event("product name 1", 123.123f, Timestamp.valueOf("2018-09-11 12:00:00.123"),
      "category 1", "127.0.0.1")
    assert(importer.importData().collect()(0) == expectedEvent)
  }
}
