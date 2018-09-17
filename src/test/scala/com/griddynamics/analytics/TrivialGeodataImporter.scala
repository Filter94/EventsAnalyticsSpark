package com.griddynamics.analytics

import com.griddynamics.analytics.importers.{CountryBlockImporter, CountryLocationsImporter}

trait TrivialGeodataImporter extends LocalSparkContext {
  protected val clImporter: CountryLocationsImporter = {
    val coutryLocationsPath: String = getClass.getResource("/geodata/cl_test.csv").getPath
    CountryLocationsImporter(spark, coutryLocationsPath)
  }
  protected val cbImporter: CountryBlockImporter = {
    val clountryBlocksPath: String = getClass.getResource("/geodata/cb_test.csv").getPath
    CountryBlockImporter(spark, clountryBlocksPath)
  }
}
