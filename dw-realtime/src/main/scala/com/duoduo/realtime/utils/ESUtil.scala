package com.duoduo.realtime.utils

import java.util
import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

/**
 * Author z
 * Date 2020-08-27 09:19:55
 */
object ESUtil {
  private var factory: JestClientFactory = null
  private val ES_HOST = "http://39.98.49.211"
  private val ES_HTTP_PORT = 9200
  
  /**
   * 获取客户端
   *
   * @return 客户端对象
   */
  def getClient = {
    if (factory == null)
      build()
    factory.getObject
  }
  
  /**
   * 关闭客户端对象
   *
   * @param c
   */
  def close(c: JestClient): Unit = {
    if (!Objects.isNull(c))
      try
        c.close()
      catch {
        case e: Exception => e.printStackTrace()
      }
  }
  
  /**
   * 构建连接
   */
  private def build() = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(
      new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT)
        .multiThreaded(true) //允许多线程
        .maxTotalConnection(20) //最大连接数
        .connTimeout(10000) //连接超时时间
        .readTimeout(10000) //读取超时时间
        .build()
    )
  }
  
  def insertBulk(indexName: String, list: List[(String, Any)]) = {
    if (list.nonEmpty && list.size > 0) {
      val jest = getClient
      val builder = new Bulk.Builder()
      // .defaultIndex(indexName).defaultType("_doc")
      for ((id, source) <- list) {
        val index = new Index
        .Builder(source)
          .index(indexName)
          .`type`("_doc")
          .id(id)
          .build()
        builder.addAction(index)
      }
      val items: util.List[BulkResult#BulkResultItem] = jest
        .execute(builder.build()).getItems
      println(s"保存=${items.size()}")
      close(jest)
    }
  }
  
  def main(args: Array[String]): Unit = {
    val jest: JestClient = getClient
    val source = "{\n  \"name\":\"li4\",\n  \"age\":456,\n  \"amount\": 250.1,\n  \"phone_num\":\"138***2123\"\n}"
    val index: Index = new Index.Builder(source).index("test").`type`("_doc").build()
    jest.execute(index)
    close(jest)
  }
}
