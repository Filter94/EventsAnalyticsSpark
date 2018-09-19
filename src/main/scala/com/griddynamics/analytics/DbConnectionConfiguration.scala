package com.griddynamics.analytics

import java.util.Properties

object DbConnectionConfiguration {
  def apply(propertiesPath: String): DbConnectionConfiguration =
    new DbConnectionConfiguration(propertiesPath)
}

class DbConnectionConfiguration(propertiesPath: String) {
  val properties = new Properties()
  properties.load(getClass.getClassLoader.getResourceAsStream(propertiesPath))
  val jdbcUrl: String = properties.getProperty("jdbcUrl")
}
