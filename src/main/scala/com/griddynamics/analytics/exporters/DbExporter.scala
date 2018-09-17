package com.griddynamics.analytics.exporters

import com.griddynamics.analytics.DbConnectionConfiguration
import org.apache.spark.sql.{Dataset, SaveMode}

object DbExporter {
  def apply(dbConfig: DbConnectionConfiguration): DbExporter = new DbExporter(dbConfig)
}

class DbExporter(dbConfig: DbConnectionConfiguration) {
  def exportDsToDbTable[T](ds: Dataset[T], tableName: String, saveMode: SaveMode): Unit = {
    ds.write
      .mode(saveMode)
      .jdbc(dbConfig.jdbcUrl, tableName, dbConfig.properties)
  }
}
