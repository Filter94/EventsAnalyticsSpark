package com.griddynamics.analytics

import org.apache.spark.rdd.RDD

object RddEventsAndGeoDataAnalytics {
  def apply(events: RDD[Event], cb: RDD[CountryBlock],
            cl: RDD[CountryLocation]): RddEventsAndGeoDataAnalytics =
    new RddEventsAndGeoDataAnalytics(events, cb, cl)
}

class RddEventsAndGeoDataAnalytics(events: RDD[Event], cb: RDD[CountryBlock], cl: RDD[CountryLocation]) {

}
