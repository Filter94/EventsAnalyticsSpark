package com.griddynamics.analytics

import org.scalatest.FunSuite
import com.griddynamics.analytics.SparkContextKeeper.spark
import com.griddynamics.analytics.importers.EventsImporter
import org.apache.spark.sql.Dataset
import spark.implicits._

class DsEventsAndGeodataAnalyticsTest extends FunSuite with TrivialEventsImporter with TrivialGeodataImporter {
  val cb: Dataset[CountryBlock] = cbImporter.importData()
  val cl: Dataset[CountryLocation] = clImporter.importData()
  val trivialAnalytics = DsEventsAndGeoDataAnalytics(eventsImporter.importData(), cb, cl)

  test("Geodata DS join works on trivial input") {
    val events: Dataset[Event] = spark.createDataset[Event](Seq())
    val result = DsEventsAndGeoDataAnalytics(events, cb, cl).necessaryJoinedGeodata().collect()
    assert(result.length == 4)
    assert(result.toSet == Set(
      ("1.0.0.0/24", "Руанда"), ("1.0.1.0/24", "Сомали"),
      ("1.0.2.0/23", "Сомали"), ("3.2.2.0/23", "ЮАР")))
  }

  test("Events and Geodata DS join works on trivial input") {
    val result = trivialAnalytics.necessaryJoinedEventsDataWithGeodata()
      .collect()
    assert(result.length == 2)
    assert(result.toSet == Set((123.123f, "Руанда"), (123.123f, "Сомали")))
  }

  test("top 10 countries by sells works correct trivial input") {
    val result = trivialAnalytics.necessaryJoinedEventsDataWithGeodata()
      .collect()
    assert(result.length == 2)
    assert(result.toSet == Set((123.123f, "Руанда"), (123.123f, "Сомали")))
  }

  test("top 10 countries by sells works correct complex input") {
    val complexCasePath = getClass.getResource("/events/complex.csv").getPath
    val complexEvents = EventsImporter(complexCasePath).importData()
    val result = DsEventsAndGeoDataAnalytics(complexEvents, cb, cl)
      .top10CountriesBySells()
      .collect()
    assert(result.length == 3)
    assert(result ===  Array((1002d, "Руанда"), (1001d, "Сомали"), (55d, "ЮАР")))
  }
}
