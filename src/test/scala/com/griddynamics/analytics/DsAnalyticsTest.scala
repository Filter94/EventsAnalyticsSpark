package com.griddynamics.analytics

import org.scalatest.FunSuite

class DsAnalyticsTest extends FunSuite with TestEventsImporter {
  val analytics = DsAnalytics(importer.importData())

  test("topCategories works well on trivial input") {
    val result = analytics.topCategories().collect()
    assert(result.length == 2)
    assert(result sameElements Array(("category 1", 2), ("category 2", 1)))
  }
}
