package com.ppetrov

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val (year, month, week) = validateDate(args)
    val spark = SparkSession.builder().master("local[2]").appName("SparkTestTask").getOrCreate()
    ShowcaseService.init(spark, year, month, week)
  }

  def validateDate(args: Array[String]) = {
    try {
//      (args(0).toInt, args(2).toInt, args(3).toInt)
      (2017, 8, 1)
    } catch {
      case _: Exception => throw new Exception("No valid input params: should be 3 of them: year month week")
    }
  }
}
