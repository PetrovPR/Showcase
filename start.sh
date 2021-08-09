#!/bin/bash
echo "You can use 3 agruments: year in yyyy fromat, month in mm format and week number in year"
echo "Year: $1";
echo "Month: $2";
echo "Week: $3";

year=$1;
month=$2
week=$3

if [ -z "$year" ]; then
  year=0;
fi

if [ -z "$month" ]; then
  month=0;
fi

if [ -z "$week" ]; then
  week=0;
fi

cd ./target/scala-2.11

spark-submit --class com.ppetrov.Main SparkTestTask-assembly-0.1.jar year month week

