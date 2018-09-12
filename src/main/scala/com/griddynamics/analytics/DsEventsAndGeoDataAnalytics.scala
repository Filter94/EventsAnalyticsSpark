package com.griddynamics.analytics

import com.griddynamics.analytics.SparkContextKeeper.spark
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import spark.implicits._

object DsEventsAndGeoDataAnalytics {
  def apply(events: Dataset[Event], cb: Dataset[CountryBlock],
            cl: Dataset[CountryLocation]): DsEventsAndGeoDataAnalytics =
    new DsEventsAndGeoDataAnalytics(events, cb, cl)
}

class DsEventsAndGeoDataAnalytics(events: Dataset[Event], cb: Dataset[CountryBlock], cl: Dataset[CountryLocation]) {
  def necessaryJoinedGeodata(): Dataset[(Network, CountryName)] = {
    cb
      .join(cl, cb.col("geoname_id") === cl.col("geoname_id"))
      .select($"network".as[Network], $"country_name".as[CountryName])
  }
}
