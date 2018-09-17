package com.griddynamics.analytics

import org.apache.spark.sql.SparkSession

trait LocalSparkContext {
  val spark: SparkSession = SparkSession.builder()
    .appName("Geodata analytics")
    .config("spark.master", "local[*]")
    .getOrCreate()
}
