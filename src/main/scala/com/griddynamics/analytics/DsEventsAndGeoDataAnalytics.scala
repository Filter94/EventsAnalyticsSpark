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

  def necessaryJoinedEventsDataWithGeodata(): Dataset[(ProductPrice, CountryName)] = {
    events.join(necessaryJoinedGeodata(),
      Helper.ipInNetUdf($"clientIp".as[Ip], $"network".as[Network]))
      .select($"productPrice".as[ProductPrice], $"country_name".as[CountryName])
  }

  def top10CountriesBySells(): Dataset[(Sells, CountryName)] = {
    necessaryJoinedEventsDataWithGeodata()
      .groupBy($"country_name".as[String])
      .agg(sum($"productPrice").as("sells"))
      .select($"sells".as[Sells], $"country_name".as[CountryName])
      .orderBy($"sells" desc)
  }
}
