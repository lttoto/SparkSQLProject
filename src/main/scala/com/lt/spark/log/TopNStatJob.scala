package com.lt.spark.log


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Created by taoshiliu on 2018/2/17.
  * TopN统计Spark作业
  */
object TopNStatJob {
  def main(args: Array[String]) {
    //config("spark.sql.sources.partitionColumnTypeIntference.enabled","false")该参数表示转换时，不使用推测，保证Schema不会自动转换成非定义的类型
    val spark = SparkSession.builder().appName("TopNStatJob").config("spark.sql.sources.partitionColumnTypeIntference.enabled","false").master("local[2]").getOrCreate()

    val accessDF = spark.read.format("parquet").load("清洗Job处理完以后保存的文件地址")
    /*accessDF.printSchema()
    accessDF.show()*/

    //统计前先删除表中数据
    StatDAO.deleteData("20170511")

    //最受欢迎的TopN课程
    videoAccessTopNStat(spark,accessDF)

    //按照地市进行统计TopN课程
    cityAccessTopNStat(spark,accessDF)

    //按照流量进行统计TopN课程
    videoTrafficsTopNStat(spark,accessDF)

    spark.stop()
  }

  //按照流量进行统计TopN课程
  def videoTrafficsTopNStat(spark: SparkSession,accessDF:DataFrame): Unit = {
    import spark.implicits._
    val cityAccessTopNDF = accessDF.filter($"day" === "20170511" && $"cmsType" === "video").groupBy("day","cmsId").agg(sum("traffic").as("traffics")).orderBy($"traffics".desc)
    try {
      cityAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoTrafficsStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")
          list.append(DayVideoTrafficsStat(day,cmsId,traffics))
        })
        StatDAO.insertDayVideoTrafficsAccessTopN(list)
      })
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }

  //按照地市进行统计TopN课程
  def cityAccessTopNStat(spark: SparkSession,accessDF:DataFrame): Unit = {
    import spark.implicits._
    val cityAccessTopNDF = accessDF.filter($"day" === "20170511" && $"cmsType" === "video").groupBy("day","city","cmsId").agg(count("cmsId").as("times"))

    //Window函数在SparkSQL的使用(和SQL一样)
    val top3DF = cityAccessTopNDF.select(cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
      .orderBy(cityAccessTopNDF("times").desc)
      ).as("times_rank")
    ).filter("times_rank <= 3")

    try {
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")
          list.append(DayCityVideoAccessStat(day,cmsId,city,times,timesRank))
        })
        StatDAO.insertDayCityVideoAccessTopN(list)
      })
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }


  //最受欢迎的TopN课程
  def videoAccessTopNStat(spark: SparkSession,accessDF:DataFrame):Unit = {
    //一、使用DATAFRAME API实现
    import spark.implicits._
    val videoAccessTopNDF = accessDF.filter($"day" === "20170511" && $"cmsType" === "video").groupBy("day","cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)
    //videoAccessTopNDF.show()

    //二、使用SparkSQL API实现
    /*accessDF.createGlobalTempView("access_log")
    val videoAccessTopNDF = spark.sql("select day,cmsId,count(1) from access_log where day = '20170511' and cmsType = 'video' group by day,cmsId order by times desc")
    videoAccessTopNDF.show()*/

    //将统计结果存入Mysql
    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")
          list.append(DayVideoAccessStat(day,cmsId,times))
        })
        StatDAO.insertDayVideoAccessTopN(list)
      })
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }


}
