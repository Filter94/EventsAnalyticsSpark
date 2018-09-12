package com.griddynamics.analytics

import com.griddynamics.analytics.SparkContextKeeper.spark
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import spark.implicits._
import org.apache.spark.sql.functions.udf

object DsEventsAndGeoDataAnalytics {
  def apply(events: Dataset[Event], cb: Dataset[CountryBlock],
            cl: Dataset[CountryLocation]): DsEventsAndGeoDataAnalytics =
    new DsEventsAndGeoDataAnalytics(events, cb, cl)

  val ipInNetFunc: (Ip, Network) => Boolean = Helper.ipInNet
  val ipInNetUdf: UserDefinedFunction = udf(ipInNetFunc)
}

class DsEventsAndGeoDataAnalytics(events: Dataset[Event], cb: Dataset[CountryBlock], cl: Dataset[CountryLocation]) {
  def necessaryJoinedGeodata(): Dataset[(Network, CountryName)] = {
    cb
      .join(cl, cb.col("geoname_id") === cl.col("geoname_id"))
      .select($"network".as[Network], $"country_name".as[CountryName])
  }

  def necessaryJoinedEventsDataWithGeodata(): Dataset[(ProductPrice, CountryName)] = {
    events.join(necessaryJoinedGeodata(),
      DsEventsAndGeoDataAnalytics.ipInNetUdf($"clientIp".as[Ip], $"network".as[Network]))
      .select($"productPrice".as[ProductPrice], $"country_name".as[CountryName])
  }
}
