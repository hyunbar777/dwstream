package com.duoduo.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.duoduo.realtime.utils.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author z
 * Date 2020-08-28 15:32:34
 */
object MyTest {
  def main(args: Array[String]): Unit = {
    //1.0 构建spark环境
    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "gmall_consumer_group"
    val topic = "GMALL_START"
    val startupInputDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(topic, ssc)
  
  
    val startLogInfoDStream: DStream[JSONObject] = startupInputDstream.map { record =>
      val startupJson: String = record.value()
      val startupJSONObj: JSONObject = JSON.parseObject(startupJson)
      val ts = startupJSONObj.getLong("ts")
      startupJSONObj
    }
    startLogInfoDStream.print(100)
    ssc.start()
    ssc.awaitTermination()
  }
}
