package com.griddynamics

import java.sql.Timestamp

import org.apache.spark.sql.types._

package object analytics {
  type Category = String
  type ProductName = String
  type ProductPrice = Float
  type Sells = Double
  type Ip = String

  object Event {
    val schema: StructType = StructType(
      Seq(
        StructField("productName", StringType, nullable = false),
        StructField("productPrice", FloatType, nullable = true),
        StructField("purchaseDate", TimestampType, nullable = false),
        StructField("productCategory", StringType, nullable = false),
        StructField("clientIp", StringType, nullable = false)
      )
    )
  }

  case class Event(productName: String, productPrice: ProductPrice, purchaseDate: Timestamp,
                   productCategory: Category, clientIp: Ip)

  object CountryBlock {
    val schema: StructType = StructType(
      Seq(
        StructField("network", StringType, nullable = false),
        StructField("geoname_id", IntegerType, nullable = false),
        StructField("registered_country_geoname_id", IntegerType, nullable = true),
        StructField("represented_country_geoname_id", IntegerType, nullable = true),
        StructField("is_anonymous_proxy", IntegerType, nullable = false),
        StructField("is_satellite_provider", IntegerType, nullable = false)
      )
    )
  }

  type Network = String
  type GeonameId = Int
  type CountryName = String

  case class CountryBlock(network: Network, geoname_id: GeonameId, registered_country_geoname_id: Option[Int],
                          represented_country_geoname_id: Option[Int], is_anonymous_proxy: Int, is_satellite_provider: Int)

  object CountryLocation {
    val schema: StructType = StructType(
      Seq(
        StructField("geoname_id", IntegerType, nullable = false),
        StructField("locale_code", StringType, nullable = false),
        StructField("continent_code", StringType, nullable = false),
        StructField("continent_name", StringType, nullable = false),
        StructField("country_iso_code", StringType, nullable = false),
        StructField("country_name", StringType, nullable = false),
        StructField("is_in_european_union", IntegerType, nullable = false)
      )
    )
  }

  case class CountryLocation(geoname_id: GeonameId, locale_code: String, continent_code: String, continent_name: String,
                             country_iso_code: String, country_name: CountryName, is_in_european_union: Int)
}
