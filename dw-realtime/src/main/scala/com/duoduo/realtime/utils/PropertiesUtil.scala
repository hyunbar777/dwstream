package com.duoduo.realtime.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * Author z
 * Date 2020-08-27 10:04:21
 */
object PropertiesUtil {
  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("config.properties")
  }
  def load(propertiesName: String) = {
    val p=new Properties()
      p.load(new InputStreamReader(
        Thread.currentThread().getContextClassLoader
          .getResourceAsStream(propertiesName)
        , "UTF-8"))
    p
  }
}
