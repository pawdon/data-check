package com.pawdon.datacheck

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.log.level", "WARN")
      .getOrCreate()
  }

  def assertDataFramesEqual(df1: DataFrame, df2: DataFrame): Unit = {
    val collect = (df: DataFrame) => df.orderBy(df.columns.map(col): _*).collect()
    assert(df1.columns sameElements df2.columns, "Columns should be equal")
    assert(collect(df1) sameElements collect(df2), "Rows should be equal")
  }
}
