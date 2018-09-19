  package com.griddynamics.analytics

  import org.apache.spark.sql.{Dataset, SparkSession}
  import org.apache.spark.sql.functions._

  object DsEventsAndGeoDataAnalytics {
    def apply(spark: SparkSession, events: Dataset[Event], cb: Dataset[CountryBlock],
              cl: Dataset[CountryLocation]): DsEventsAndGeoDataAnalytics =
      new DsEventsAndGeoDataAnalytics(spark, events, cb, cl)
  }

  class DsEventsAndGeoDataAnalytics(private val spark: SparkSession, private val events: Dataset[Event],
                                    private val cb: Dataset[CountryBlock], private val cl: Dataset[CountryLocation]) {
    import spark.implicits._

    def necessaryJoinedGeodata(): Dataset[(Network, CountryName)] = {
      cb
        .join(cl, cb.col("geoname_id") === cl.col("geoname_id"))
        .select($"network".as[Network], $"country_name".as[CountryName])
    }

    def necessaryJoinedEventsDataWithGeodata(): Dataset[(ProductPrice, CountryName)] = {
      events
        .join(necessaryJoinedGeodata(),
        Helper.ipInNetUdf($"clientIp", $"network"))
        .select($"productPrice".as[ProductPrice], $"country_name".as[CountryName])
    }

    def top10CountriesBySells(): Dataset[(Sells, CountryName)] = {
      necessaryJoinedEventsDataWithGeodata()
        .groupBy($"country_name".as[String])
        .agg(sum($"productPrice").as("sells"))
        .select($"sells".as[Sells], $"country_name".as[CountryName])
        .orderBy($"sells" desc)
        .limit(10)
    }
  }
