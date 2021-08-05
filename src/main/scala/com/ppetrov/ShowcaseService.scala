package com.ppetrov

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, last_day}

/**
 *
 * Show case service  - join two tabllses
 */
object ShowcaseService {

  def init(spark: SparkSession): Unit = {

    val txn = spark.read.parquet("data/custom/rb/card/pa/txn").repartition(col("trx_date"))
    val epkLnkHostIdDf = spark.read.parquet("data/custom/rb/epk/pa/epk_lnk_host_id").repartition(col("row_actual_to"))

    txn.join(epkLnkHostIdDf, epkLnkHostIdDf("external_system_client_id") === txn("client_w4_id"))
      .filter(col("external_system") === "WAY4")
      .filter("to_date(trx_date) <= to_date(row_actual_to) and to_date(trx_date) >= to_date(row_actual_from)")
      .select(col("external_system") as "sum_txn", col("mcc_code"), col("epk_id"))
      .withColumn("report_dt", last_day(col("trx_date")))
      .write
      .partitionBy("report_dt")
      .parquet("data/custom/rb/txn_aggr/pa/txn_agg")
  }
}
