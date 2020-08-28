package com.duoduo.realtime.app

import com.alibaba.fastjson.JSON
import com.duoduo.realtime.utils.{KafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * Author z
 * Date 2020-08-28 21:21:58
 */
object BaseDBMaxwellAPP {
  def main(args: Array[String]): Unit = {
    //1.0 构建spark环境
    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("BaseDBMaxwellAPP")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_DB_GMALL_Maxwell"
    val groupId = "ODS_DB_GMALL_Maxwell_GROUP"
    
    //2.0 从redis中读取偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
    var dbDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffsetMap != null && kafkaOffsetMap.size > 0) {
      dbDstream = KafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    } else {
      dbDstream = KafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    //3.0 将数据流转换为rdd,从中获取当前批次的Kafka偏移量
    val getOffsetDstream: DStream[ConsumerRecord[String, String]] = dbDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    val jsonDstream = getOffsetDstream.map { record =>
      val jsonString = record.value()
      val jsonObj = JSON.parseObject(jsonString)
      jsonObj
    }
    
    jsonDstream.foreachRDD { rdd =>
      rdd.foreachPartition { iter =>
        if (offsetRanges != null && offsetRanges.size > 0) {
          val offsetRange: OffsetRange = offsetRanges(TaskContext.get().partitionId())
          println("fromOffset:" + offsetRange.fromOffset + "--untilOffset:" + offsetRange.untilOffset)
        }
        for (json <- iter) {
          if (!json.getString("type").equals("bootstrap-start") &&
            !json.getString("type").equals("bootstrap-complete")) {
            val table = json.getString("table")
            val jsonArr = json.getJSONArray("data")
            val topic = "ODS_Maxwell_" + table.toUpperCase
            val key = table + "_" + json.getString("id")
            KafkaUtil.send(topic, key, json.toJSONString)
          }
        }
      }
      // 偏移量移动位置写入redis
      OffsetManager.submitOffset(topic, groupId, offsetRanges)
    }
    
    ssc.start()
    ssc.awaitTermination()
  }
}
