package com.duoduo.realtime.utils

import java.sql.DriverManager
import java.util.Properties

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
 * Author z
 * Date 2020-08-28 21:36:39
 */
object PhoenixUtil {
  private val properties: Properties = PropertiesUtil.load("config.properties")
  
  /**
   * 从phoenix查询数据
   * @param sql 语句
   * @return
   */
  def queryList(sql: String) = {
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val resultList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
    val connection = DriverManager.getConnection(properties.getProperty("phoenix.url"))
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)
    val resultMetaData = resultSet.getMetaData
    while (resultSet.next()) {
      val rowData = new JSONObject()
      for (i <- 1 to resultMetaData.getColumnCount) {
        rowData.put(resultMetaData.getColumnName(i), resultSet.getObject(i))
      }
      resultList += rowData
    }
    statement.close()
    connection.close()
    resultList.toList
  }
}
