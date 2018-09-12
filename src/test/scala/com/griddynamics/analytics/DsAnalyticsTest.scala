package com.griddynamics.analytics

import org.scalatest.FunSuite

class DsAnalyticsTest extends FunSuite with TestEventsImporter {
  val analytics = DsAnalytics(importer.importData())

  test("topCategories DS works well on trivial input") {
    val result = analytics.topCategories().collect()
    assert(result.length == 2)
    assert(result sameElements Array(("category 1", 2), ("category 2", 1)))
  }

  test("top10ByCategory DS works well on trivial input") {
    val result = analytics.top10ByCategory().collect()
    val expected: Set[(Category, ProductName, DsAnalytics.Purchases)] = Set(
      ("category 1", "product name 1", 1),
      ("category 1", "product name 2", 1),
      ("category 2", "product name 3", 1))
    assert(result.length == 3)
    assert(result.toSet == expected)
  }

  test("top10ByCategory DS works well on some more complex input") {
    val complexCasePath = getClass.getResource("/complex_case").getPath
    val analytics = DsAnalytics(EventsImporter(complexCasePath).importData())
    val result = analytics.top10ByCategory().collect()
    assert(result.length == 14)
    val category1 = result.filter{case (category, _, _) => category == "category 1"}
    assert(category1.length == 10)
    assert(category1.sortBy{case (_, _, purchases) => -purchases} sameElements category1)
  }
}