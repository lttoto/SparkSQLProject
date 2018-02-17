package com.lt.spark.log

import org.apache.spark.sql.SparkSession

/**
  * Created by taoshiliu on 2018/2/17.
  * 第一步数据清洗
  *
  */
object SparkStatFormatJob {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

    val access = spark.sparkContext.textFile("日志路径：可以是file：///本地，或者是hdfs：///分布式集群HDFS")
    //access.take(10).foreach(println)
    access.map(line => {

      val splits = line.split(" ")
      val ip = splits(0)
      val time = DateUtils.parse(splits(3) + " " + splits(4))
      val url = splits(11).replace("\"","")
      val traffic = splits(9)

      time + "\t" + url + "\t" + traffic + "\t" + ip

    }).saveAsTextFile("保存的路径：可以是file：///本地，或者是hdfs：///分布式集群HDFS")


    spark.stop()
  }
}
