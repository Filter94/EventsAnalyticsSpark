package com.griddynamics.analytics

import org.apache.spark.rdd.RDD

object RddAnalytics {
  def apply(events: RDD[Event]): RddAnalytics = new RddAnalytics(events)
}

class RddAnalytics(events: RDD[Event]) {
  type Purchases = Int

  def topCategories(): RDD[(Category, Purchases)] = {
    events
      .groupBy(e => e.productCategory)
      .map{
        case (category, group) =>
          (category, group.count(_ => true))
      }
      .sortBy{
        case (_, purchases) => -purchases
      }
  }
}
