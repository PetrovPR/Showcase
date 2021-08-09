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
      if(args(0).toInt == 0) {
        throw new Exception("Please set even the year")
      }

      (args(0).toInt, args(2).toInt, args(3).toInt)
    } catch {
      case _: Exception => throw new Exception("No valid input params: should be 3 of them: year month week")
    }
  }
}
