package com.griddynamics

import java.sql.Timestamp

import org.apache.spark.sql.types._

package object analytics {
  type Category = String
  type ProductName = String

  object Event {
    def schema: StructType = StructType(
      Seq(
        StructField("productName", StringType, nullable = false),
        StructField("productPrice", FloatType, nullable = true),
        StructField("purchaseDate", TimestampType, nullable = false),
        StructField("productCategory", StringType, nullable = false),
        StructField("clientIp", StringType, nullable = false)
      )
    )
  }

  case class Event(productName: String, productPrice: Float, purchaseDate: Timestamp,
                   productCategory: String, clientIp: String)
}
