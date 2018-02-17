package com.lt.spark.log

import java.sql.{PreparedStatement, Connection}

import scala.collection.mutable.ListBuffer

/**
  * Created by taoshiliu on 2018/2/17.
  * 类似JAVA的DAO
  */
object StatDAO {
  def insertDayVideoAccessTopN(list:ListBuffer[DayVideoAccessStat]):Unit = {

    var connection:Connection = null
    var pstmt:PreparedStatement = null

    try{
      connection = MySQLUtil.getConnection()
      connection.setAutoCommit(false)
      val sql = "insert into day_video_access_topn_stat(day,cms_id,times) values (?,?,?) "
      pstmt = connection.prepareStatement(sql)

      for(ele <- list) {
        pstmt.setString(1,ele.day)
        pstmt.setLong(2,ele.cmsId)
        pstmt.setLong(3,ele.times)
        pstmt.addBatch()
      }
      pstmt.executeBatch() //执行批量操作
      connection.commit() //手工commit
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      MySQLUtil.release(connection,pstmt)
    }

  }

  def insertDayCityVideoAccessTopN(list:ListBuffer[DayCityVideoAccessStat]):Unit = {

    var connection:Connection = null
    var pstmt:PreparedStatement = null

    try{
      connection = MySQLUtil.getConnection()
      connection.setAutoCommit(false)
      val sql = "insert into day_video_city_access_topn_stat(day,cms_id,city,times,times_rank) values (?,?,?,?,?) "
      pstmt = connection.prepareStatement(sql)

      for(ele <- list) {
        pstmt.setString(1,ele.day)
        pstmt.setLong(2,ele.cmsId)
        pstmt.setString(3,ele.city)
        pstmt.setLong(4,ele.times)
        pstmt.setInt(5,ele.timesRank)
        pstmt.addBatch()
      }
      pstmt.executeBatch() //执行批量操作
      connection.commit() //手工commit
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      MySQLUtil.release(connection,pstmt)
    }

  }

  def insertDayVideoTrafficsAccessTopN(list:ListBuffer[DayVideoTrafficsStat]):Unit = {

    var connection:Connection = null
    var pstmt:PreparedStatement = null

    try{
      connection = MySQLUtil.getConnection()
      connection.setAutoCommit(false)
      val sql = "insert into day_video_traffics_access_topn_stat(day,cms_id,traffics) values (?,?,?) "
      pstmt = connection.prepareStatement(sql)

      for(ele <- list) {
        pstmt.setString(1,ele.day)
        pstmt.setLong(2,ele.cmsId)
        pstmt.setLong(3,ele.traffics)
        pstmt.addBatch()
      }
      pstmt.executeBatch() //执行批量操作
      connection.commit() //手工commit
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      MySQLUtil.release(connection,pstmt)
    }

  }

  def deleteData(day:String):Unit = {
    val tables = Array("day_video_access_topn_stat","day_video_city_access_topn_stat","day_video_traffics_access_topn_stat")
    var connection:Connection = null
    var pstmt:PreparedStatement = null

    try{
      connection = MySQLUtil.getConnection()

      for(table <- tables) {
        val deleteSql = s"delete from $table where day = ?"
        pstmt = connection.prepareStatement(deleteSql)
        pstmt.setString(1,day)
        pstmt.executeUpdate()
      }
    }catch {
      case e :Exception => e.printStackTrace()
    }finally {
      MySQLUtil.release(connection,pstmt)
    }
  }
}
