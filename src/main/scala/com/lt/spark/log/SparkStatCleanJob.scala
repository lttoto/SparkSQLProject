package com.lt.spark.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by taoshiliu on 2018/2/17.
  * 第二步数据清洗操作
  */
object SparkStatCleanJob {
  def main(args: Array[String]) {
    //.config("spark.sql.parquet.compression.codec","gzip")支持不用的压缩的格式，可以自由定义
    val spark = SparkSession.builder().appName("SparkStatCleanJob").config("spark.sql.parquet.compression.codec","gzip").master("local[2]").getOrCreate()

    //SparkOnYarn
   /* if(args.length != 2) {
      println("Usage:SparkStatCleanJob <input><output>")
    }
    val Array(inputPath,outputPath) = args
    val spark = SparkSession.builder().getOrCreate()
    val accessRDD = spark.sparkContext.textFile(inputPath)
    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save(outputPath)*/

    val accessRDD = spark.sparkContext.textFile("读取SparkStatFormatJob执行后的进过第一步清洗的文件")

    //RDD转换为DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),AccessConvertUtil.struct)
    /*accessDF.printSchema()
    accessDF.show(false)*/

    //存储结果DF
    //partitionBy表示文件的存储的分区，例如 1、20170511的记录=>保存在文件夹名为day=20170511; 2、20170512的记录=>保存在文件夹名为day=20170512（分区对于之后的数据处理相当关键，要符合最后查询的逻辑）
    //coalesce(n)表示将文件存储在n文件中,具体n的设置要根据文件大小分析
    //mode()表示覆盖保存
    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save("清洗数据的存储地址")

    spark.stop()
  }
}
