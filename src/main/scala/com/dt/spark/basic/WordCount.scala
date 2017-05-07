package com.dt.spark.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 17-5-7.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]");
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("hdfs://MacMaster:9000/data/word.txt")
    val wordCount = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    val wordSout = wordCount.map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1))
    wordSout.collect().foreach(println)
  }
}
