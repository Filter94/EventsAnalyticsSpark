package com.griddynamics.analytics

import com.griddynamics.analytics.importers.{CountryBlockImporter, CountryLocationsImporter}

trait TrivialGeodataImporter {
  protected val clImporter: CountryLocationsImporter = {
    val coutryLocationsPath: String = getClass.getResource("/geodata/cl_test.csv").getPath
    CountryLocationsImporter(coutryLocationsPath)
  }
  protected val cbImporter: CountryBlockImporter = {
    val clountryBlocksPath: String = getClass.getResource("/geodata/cb_test.csv").getPath
    CountryBlockImporter(clountryBlocksPath)
  }
}
