package com.duoduo.realtime.utils

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

/**
 * Author z
 * Date 2020-08-28 09:44:34
 */
object OffsetManager {
  
  /**
   * 从redis中获取偏移量
   *
   * @param topicName
   * @param groupId
   * @return
   */
  def getOffset(topicName: String, groupId: String): Map[TopicPartition, Long] = {
    val jedisClient = RedisUtil.getJedisClient
    val offsetKey = "offset:" + topicName + ":" + groupId
    //kafka需要的格式
    var offsetMap: Map[TopicPartition, Long] = Map[TopicPartition, Long]()
    //从redis中获取偏移量
    val redisOffsetMap: util.Map[String, String] = jedisClient.hgetAll(offsetKey)
    jedisClient.close()
    
    import scala.collection.JavaConversions._
    
    if (redisOffsetMap != null && redisOffsetMap.size() > 0) {
      println("从redis获取偏移量："+ redisOffsetMap.toList(0)._2)
      //将从redis中获取的偏移量格式转换为kafka需要的格式
      offsetMap = redisOffsetMap.map {
        case (partition, offset) =>
          println("加载分区偏移量：" + partition + ", 偏移量位置：" + offset)
          (new TopicPartition(topicName, partition.toInt), offset.toLong)
      }.toMap
    }
    offsetMap
  }
  
  def submitOffset(topicName: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    if (offsetRanges != null && offsetRanges.size>0) {
      val offsetMap: Map[String, String] = offsetRanges.map { o =>
        val partition = o.partition
        val untilOffset = o.untilOffset
        println("分区号：" + partition + ", 偏移量开始位置：" + o.fromOffset + ", 偏移量结束位置：" + untilOffset)
        (partition.toString, untilOffset.toString)
      }.toMap
      
      import scala.collection.JavaConversions._
      val jedisClient = RedisUtil.getJedisClient
      
      jedisClient.hmset("offset:" +topicName  + ":" +groupId , offsetMap)
      jedisClient.close()
    }
  }
}
