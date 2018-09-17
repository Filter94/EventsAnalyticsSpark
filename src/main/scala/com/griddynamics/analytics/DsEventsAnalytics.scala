package com.griddynamics.analytics

import org.apache.spark.sql.{Dataset, SparkSession}
import com.griddynamics.analytics.DsEventsAnalytics.Purchases
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object DsEventsAnalytics {
  type Purchases = Long

  def apply(spark: SparkSession, events: Dataset[Event]): DsEventsAnalytics =
    new DsEventsAnalytics(spark, events)
}

class DsEventsAnalytics(spark: SparkSession, events: Dataset[Event]) {
  import spark.implicits._

  def top10Categories(): Dataset[(Category, DsEventsAnalytics.Purchases)] = {
    events
      .groupBy($"productCategory")
      .count()
      .select($"productCategory".as[Category], $"count".alias("purchases").as[DsEventsAnalytics.Purchases])
      .sort($"purchases" desc)
      .limit(10)
  }

  def top10ByCategory(): Dataset[(Category, ProductName, DsEventsAnalytics.Purchases)] = {
    val productPopularity = row_number().over(
      Window
        .partitionBy($"productCategory")
        orderBy $"purchases".desc)
    events
      .groupBy($"productCategory", $"productName")
      .agg(count($"productPrice").as("purchases"))
      .select($"productCategory", $"productName",
        $"purchases", productPopularity.as("popularity"))
      .where($"popularity" <= 10)
      .select($"productCategory".as[Category], $"productName".as[ProductName],
        $"purchases".as[Purchases])
      .orderBy($"productCategory", $"purchases" desc)
  }
}
