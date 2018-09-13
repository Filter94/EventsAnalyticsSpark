package com.griddynamics.analytics

import com.griddynamics.analytics.SparkContextKeeper.spark
import com.griddynamics.analytics.SparkContextKeeper.spark.implicits._
import org.scalatest.FunSuite

class RddEventsAndGeodataAnalyticsTest extends FunSuite with TrivialEventsImporter with TrivialGeodataImporter {
  test("Geodata RDD join works on trivial input") {
    val cb = cbImporter.importData().rdd
    val cl = clImporter.importData().rdd
    val events = spark.createDataset[Event](Seq()).rdd
    val result = RddEventsAndGeoDataAnalytics(events, cb, cl).necessaryJoinedGeodata().collect()
    assert(result.length == 4)
    assert(result.toSet == Set(
      ("1.0.0.0/24", "Руанда"), ("1.0.1.0/24", "Сомали"),
      ("1.0.2.0/23", "Сомали"), ("3.2.2.0/23", "ЮАР")))
  }
}
