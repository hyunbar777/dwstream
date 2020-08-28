package com.duoduo.realtime.utils

import java.util

import org.apache.kafka.common.TopicPartition

/**
 * Author z
 * Date 2020-08-28 14:08:56
 */
object OffsetManagerMysql {
  def getOffset(groupId: String, topic: String): Map[TopicPartition, Long] = {
    var offsetMap: Map[TopicPartition, Long] = Map[TopicPartition, Long]()
    /*val jedisClient = RedisUtil.getJedisClient
    val redisOffsetMap: util.Map[String, String] = jedisClient.hgetAll("offset:" + groupId + ":" + topic)
    jedisClient.close()*/
    val offsetJsonObjList = MysqlUtil
      .queryList("SELECT  group_id ,topic,partition_id, topic_offset  FROM offset_2020 where group_id='" + groupId + "' and topic='" + topic + "'")
    if (offsetJsonObjList.nonEmpty) {
      offsetMap = offsetJsonObjList.map { offsetJsonObj =>
        (new TopicPartition(offsetJsonObj.getString("topic"),
          offsetJsonObj.getIntValue("partition_id")),
          offsetJsonObj.getLongValue("topic_offset"))
      }.toMap
    }
    offsetMap
  }
}
