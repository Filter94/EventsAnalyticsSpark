package com.griddynamics.analytics

import org.apache.spark.sql.Dataset
import SparkContextKeeper.spark
import com.griddynamics.analytics.DsEventsAnalytics.Purchases
import org.apache.spark.sql.expressions.Window
import spark.implicits._
import org.apache.spark.sql.functions._

object DsEventsAnalytics {
  type Purchases = Long

  def apply(events: Dataset[Event]): DsEventsAnalytics = new DsEventsAnalytics(events)
}

class DsEventsAnalytics(events: Dataset[Event]) {
  def topCategories(): Dataset[(Category, DsEventsAnalytics.Purchases)] = {
    events
      .groupBy($"productCategory")
      .count()
      .select($"productCategory".as[Category], $"count".alias("purchases").as[DsEventsAnalytics.Purchases])
      .sort($"purchases" desc)
  }

  def top10ByCategory(): Dataset[(Category, ProductName, DsEventsAnalytics.Purchases)] = {
    val productPopularity = row_number().over(
      Window
        .partitionBy($"productCategory")
        orderBy $"purchases".desc)
    events
      .groupBy($"productCategory", $"productName")
      .count()
      .select($"productCategory".as[Category], $"productName".as[ProductName],
        $"count".alias("purchases").as[DsEventsAnalytics.Purchases])
      .select($"productCategory".as[Category], $"productName".as[ProductName],
        $"purchases".as[Purchases], productPopularity.as("popularity"))
      .where($"popularity" <= 10)
      .select($"productCategory".as[Category], $"productName".as[ProductName],
        $"purchases".as[Purchases])
      .orderBy($"productCategory", $"purchases" desc)
  }
}
