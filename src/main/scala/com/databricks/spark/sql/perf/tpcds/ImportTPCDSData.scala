package com.databricks.spark.sql.perf.tpcds

import org.apache.spark.sql.SparkSession

object ImportTPCDSData {
  def main(args: Array[String]): Unit = {
    val dataDir = args(0)
    val databaseName = args(1)
    val replicationFactor = args(2)
    val cassandraCatalog = "cassandra"

    val sparkSession = SparkSession
      .builder
      .appName("TPCDS QA")
      .getOrCreate()

    sparkSession.sessionState.catalogManager.setCurrentCatalog(cassandraCatalog)
    sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='$replicationFactor')")
    val tables = new TPCDSTables(
      sparkSession.sqlContext,
      dsdgenDir = "",
      scaleFactor = "",
    )
    tables.importTables(dataDir, "parquet", "cassandra", databaseName, overwrite = false)
  }
}
