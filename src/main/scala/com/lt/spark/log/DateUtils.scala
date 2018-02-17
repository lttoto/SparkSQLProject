package com.lt.spark.log

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by taoshiliu on 2018/2/17.
  * 日期解析
  * 注意：SimpleDateFormat是线程不安全的
  */
object DateUtils {

  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def parse(time:String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def getTime(time:String) = {
    try {
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1,time.lastIndexOf("]"))).getTime
    }catch {
      case e: Exception => {
        0L
      }
    }
  }

  /*def main(args: Array[String]) {
    println(parse("[10/Nov2016:00:01:02 +0800]"))
  }*/
}
