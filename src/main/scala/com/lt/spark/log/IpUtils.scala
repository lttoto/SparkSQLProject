package com.lt.spark.log

import com.ggstar.util.ip.IpHelper

/**
  * Created by taoshiliu on 2018/2/17.
  * IP工具类
  */
object IpUtils {

  def getCity(ip:String) = {
    IpHelper.findRegionByIp(ip)
  }

  //测试类
  /*def main(args: Array[String]) {
    println(getCity("xxxxx"))
  }*/

}
