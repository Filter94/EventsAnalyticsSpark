package com.griddynamics.analytics

import org.apache.spark.sql.Dataset
import SparkContextKeeper.spark
import spark.implicits._

object DsAnalytics {
  def apply(events: Dataset[Event]): DsAnalytics = new DsAnalytics(events)
}

class DsAnalytics(events: Dataset[Event]) {
  type Purchases = Long

  def topCategories(): Dataset[(Category, Purchases)] = {
    events
      .groupBy($"productCategory")
      .count()
      .select($"productCategory".as[Category], $"count".alias("purchases").as[Long])
      .sort($"purchases" desc)
  }
}
