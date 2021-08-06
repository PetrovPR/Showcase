package com.ppetrov

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
 *
 * Show case service  - join two tabllses
 */
object ShowcaseService {

  val miniFrame: mutable.Map[Any, (Dataset[Row], Dataset[Row])] = mutable.Map()

  def init(spark: SparkSession, year: Int, month: Int, week: Int): Unit = {

    //1 petabyte
    spark.read.parquet("data/custom/rb/card/pa/txn")
      .where(functions.year(to_date(col("trx_date"))) === lit(year))
      .withColumn("year", functions.year(to_date(col("trx_date"))))
      .repartition(col("year"))
      .write
      .bucketBy(31, "year")
      .sortBy("year")
      .saveAsTable("first_table_year")

    //    resultDF.explain(true)
    //    //300 gb
    spark.read.parquet("data/custom/rb/epk/pa/epk_lnk_host_id")
      .where(functions.year(to_date(col("row_actual_to"))) === lit("year"))
      .where(col("external_system") === lit("WAY4"))
      .withColumn("year", functions.year(to_date(col("row_actual_to"))))
      .repartition(col("year"))
      .write
      .bucketBy(31, "year")
      .sortBy("year")
      .saveAsTable("second_table_year")


    val d1 = spark.table("first_table_year").repartition(col("year")).toDF()
    val d2 = spark.table("second_table_year").repartition(col("year")).toDF()


    month match {
      case 0 => joinDataAndWriteFrames(d1, d2, "year")
      case _ =>
        week match {
          case 0 =>
            val tmp = spark.table("first_table_year").toDF()
            tmp.withColumn("year_month", concat(col("year"), lit("-"), functions.month(to_date(col("trx_date")))))
              .repartition(col("year"), col("year_month"))
              .write
              .bucketBy(90, "year", "year_month")
              .sortBy("year", "year_month")
              .saveAsTable("first_table_year_month")

            val tmp2 = spark.table("first_table_year").toDF()
            tmp2.withColumn("year_month", concat(col("year"), lit("-"), functions.month(to_date(col("row_actual_to")))))
              .repartition(col("year"), col("year_month"))
              .write
              .bucketBy(90, "year", "year_month")
              .sortBy("year", "year_month")
              .saveAsTable("second_table_year_month")

            joinDataAndWriteFrames(
              spark.table("first_table_year_month").repartition(col("year"), col("year_month")).toDF(),
              spark.table("second_table_year_month").repartition(col("year"), col("year_month")).toDF(),
              "year_month"
            )
          case _ =>
            val tmp = spark.table("first_table_year_month").toDF()
            tmp.withColumn("year_month_week", concat(col("year"), lit("-"),
              functions.month(to_date(col("trx_date"))), lit("-"),
              functions.weekofyear(to_date(col("trx_date")))))
              .repartition(col("year"), col("year_month"), col("year_month_week"))
              .write
              .bucketBy(180, "year", "year_month", "year_month_week")
              .sortBy("year", "year_month")
              .saveAsTable("first_table_year_month_week")

            val tmp2 = spark.table("first_table_year").toDF()
            tmp2.withColumn("year_month_week", concat(col("year"), lit("-"),
              functions.month(to_date(col("row_actual_to"))), lit("-"),
              functions.weekofyear(to_date(col("row_actual_to")))))
              .repartition(col("year"), col("year_month"), col("year_month_week"))
              .write
              .bucketBy(180, "year", "year_month", "year_month_week")
              .sortBy("year", "year_month", "year_month_week")
              .saveAsTable("second_table_year_month_week")

            joinDataAndWriteFrames(
              spark.table("first_table_year_month_week").repartition(col("year"), col("year_month"), col("year_month_week")).toDF(),
              spark.table("second_table_year_month_week").repartition(col("year"), col("year_month"), col("year_month_week")).toDF(),
              "year_month_week")
        }
    }
  }

  private def joinDataAndWriteFrames(txn: DataFrame, epk: DataFrame, partitionField: String) = {
    txn.join(epk, epk("external_system_client_id") === txn("client_w4_id"))
      .filter("to_date(trx_date) <= to_date(row_actual_to) and to_date(trx_date) >= to_date(row_actual_from)")
      .select(col("external_system") as "sum_txn", col("mcc_code"), col("epk_id"))
      .withColumn("report_dt", last_day(col("trx_date")))
      .write
      .partitionBy(partitionField)
      .parquet("data/custom/rb/txn_aggr/pa/txn_agg")
  }
}
