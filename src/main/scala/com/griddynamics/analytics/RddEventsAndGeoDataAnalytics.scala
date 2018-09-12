package com.griddynamics.analytics

import org.apache.spark.rdd.RDD

object RddEventsAndGeoDataAnalytics {
  def apply(events: RDD[Event], cb: RDD[CountryBlock],
            cl: RDD[CountryLocation]): RddEventsAndGeoDataAnalytics =
    new RddEventsAndGeoDataAnalytics(events, cb, cl)
}

class RddEventsAndGeoDataAnalytics(events: RDD[Event], cb: RDD[CountryBlock], cl: RDD[CountryLocation]) {
  def necessaryJoinedGeodata(): RDD[(Network, CountryName)] = {
    val cbPair = cb.map(countryBlock => (countryBlock.geoname_id, countryBlock.network))
    val clPair = cl.map(countryLocation => (countryLocation.geoname_id, countryLocation.country_name))
    cbPair.join(clPair).values
  }
}
