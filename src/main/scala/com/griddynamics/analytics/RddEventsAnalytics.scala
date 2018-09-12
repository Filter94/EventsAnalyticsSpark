package com.griddynamics.analytics

import org.apache.spark.rdd.RDD

object RddEventsAnalytics {
  type Purchases = Int

  def apply(events: RDD[Event]): RddEventsAnalytics = new RddEventsAnalytics(events)
}

class RddEventsAnalytics(events: RDD[Event]) {
  def topCategories(): RDD[(Category, RddEventsAnalytics.Purchases)] = {
    events
      .groupBy(e => e.productCategory)
      .map {
        case (category, group) =>
          (category, group.count(_ => true))
      }
      .sortBy {
        case (_, purchases) => -purchases
      }
  }

  def top10ByCategory(): RDD[(Category, ProductName, RddEventsAnalytics.Purchases)] = {
    val productPurchasesByCategory = for {
      (category, group) <- events.groupBy(e => e.productCategory)
      (productName, productGroup) <- group.groupBy(e => e.productName)
    } yield {
      (category, productName, productGroup.count(_ => true))
    }
    for {
      (_, categoryGroup) <- productPurchasesByCategory
        .sortBy{ case (_, _, purchases) => -purchases }
        .groupBy{ case (category, _, _) => category }
      record <- categoryGroup.take(10)
    } yield {
      record
    }
  }
}
