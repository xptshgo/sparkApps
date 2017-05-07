package com.dt.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by hadoop on 17-1-16.
  */
object SparkSqlTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSqlTest").master("spark://MacMaster:7077").getOrCreate()
    import spark.implicits._
    val df = spark.read.json("hdfs://MacMaster:9000/data/people.txt")
    df.show()
  }
}
