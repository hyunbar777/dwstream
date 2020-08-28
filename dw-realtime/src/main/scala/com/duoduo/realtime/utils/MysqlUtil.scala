package com.duoduo.realtime.utils

import java.sql.DriverManager
import java.util.Properties

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer


/**
 * Author z
 * Date 2020-08-28 11:24:19
 */
object MysqlUtil {
  def main(args: Array[String]): Unit = {
  
  }
  
  private val properties: Properties = PropertiesUtil.load("config.properties")
  
  def queryList(sql: String) = {
    Class.forName("com.mysql.jdbc.Driver")
    val resultList = new ListBuffer[JSONObject]
    val connection = DriverManager.getConnection(
      properties.getProperty("jdbc.url"),
      properties.getProperty("jdbc.root"),
      properties.getProperty("jdbc.password"))
    val statement = connection.createStatement
    val rs = statement.executeQuery(sql)
    val ms = rs.getMetaData
    while (rs.next()) {
      val rowData = new JSONObject()
      for (i <- 1 to ms.getColumnCount) {
        rowData.put(ms.getColumnName(i), rs.getObject(i))
      }
      resultList += rowData
    }
    statement.close()
    connection.close()
    resultList
  }
}
