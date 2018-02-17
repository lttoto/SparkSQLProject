package com.lt.spark.log

import java.sql.{PreparedStatement, Connection, DriverManager}

/**
  * Created by taoshiliu on 2018/2/17.
  * MySQL工具类
  */
object MySQLUtil {
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_project?user=root?password=root")
  }

  def release(connection:Connection,pstmt:PreparedStatement): Unit = {
    try{
      if(pstmt != null) {
        pstmt.close()
      }
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      connection.close()
    }
  }

  def main(args: Array[String]) {
    println(getConnection())
  }
}
