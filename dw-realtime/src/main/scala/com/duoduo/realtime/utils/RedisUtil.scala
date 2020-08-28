package com.duoduo.realtime.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}


/**
 * Author z
 * Date 2020-08-27 11:00:59
 */
object RedisUtil {
  var jedisPool: JedisPool = null
  
  def getJedisClient: Jedis = {
    if (jedisPool == null) {
      val config = PropertiesUtil.load("config.properties")
      val host = config.getProperty("redis.host")
      val port = config.getProperty("redis.port")
      val jedisPoolConfig = new JedisPoolConfig
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲
      jedisPoolConfig.setMinIdle(20) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时，是否等待
      jedisPoolConfig.setMaxWaitMillis(500) //忙碌时，等待时长，毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接进行测试
      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
    }
    jedisPool.getResource
  }
  
  def main(args: Array[String]): Unit = {
    val client = getJedisClient
    //println(client.get("dautest"))
    client.set("hello","world")
    client.close()
  }
}
