package com.griddynamics.analytics

import org.scalatest.FunSuite
import com.griddynamics.analytics.SparkContextKeeper.spark
import org.apache.spark.sql.Dataset
import spark.implicits._

class DsEventsAndGeodataAnalyticsTest extends FunSuite with TrivialEventsImporter with TrivialGeodataImporter {
  val cb: Dataset[CountryBlock] = cbImporter.importData()
  val cl: Dataset[CountryLocation] = clImporter.importData()

  test("Geodata DS join works on trivial input") {
    val events: Dataset[Event] = spark.createDataset[Event](Seq())
    val result = DsEventsAndGeoDataAnalytics(events, cb, cl).necessaryJoinedGeodata().collect()
    assert(result.length == 3)
    assert(result.toSet == Set(("1.0.0.0/24", "Руанда"), ("1.0.1.0/24", "Сомали"), ("1.0.2.0/23", "Сомали")))
  }

  test("Events and Geodata DS join works on trivial input") {
    val result = DsEventsAndGeoDataAnalytics(eventsImporter.importData(), cb, cl).necessaryJoinedEventsDataWithGeodata().collect()
    assert(result.length == 2)
    assert(result.toSet == Set((123.123f, "Руанда"), (123.123f, "Сомали")))
  }
}
