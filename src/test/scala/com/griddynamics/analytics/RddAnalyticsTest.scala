package com.griddynamics.analytics

import org.scalatest.FunSuite

class RddAnalyticsTest extends FunSuite with TestEventsImporter {
  val analytics = RddAnalytics(importer.importData().rdd)

  test("topCategories RDD works well on trivial input") {
    val result = analytics.topCategories().collect()
    assert(result.length == 2)
    assert(result sameElements Array(("category 1", 2), ("category 2", 1)))
  }
}
