package com.griddynamics.analytics

import org.scalatest.FunSuite
import com.griddynamics.analytics.SparkContextKeeper.spark
import spark.implicits._

class DsEventsAndGeodataAnalyticsTest extends FunSuite with TrivialEventsImporter with TrivialGeodataImporter {
  test("Geodata join works on trivial input") {
    val cb = cbImporter.importData()
    val cl = clImporter.importData()
    val events = spark.createDataset[Event](Seq())
    val result = DsEventsAndGeoDataAnalytics(events, cb, cl).necessaryJoinedGeodata().collect()
    assert(result.length == 3)
    assert(result sameElements Array(("1.0.0.0/24", "Руанда"), ("1.0.1.0/24", "Сомали"), ("1.0.2.0/23", "Сомали")))
  }
}
