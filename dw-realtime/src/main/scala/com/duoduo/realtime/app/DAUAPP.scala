package com.duoduo.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.duoduo.realtime.bean.DauBean
import com.duoduo.realtime.utils.{ESUtil, KafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer


/**
 * Author z
 * Date 2020-08-27 14:01:13
 */
object DAUAPP {
  def main(args: Array[String]): Unit = {
    //1.0 构建spark环境
    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "gmall_consumer_group"
    val topic = "GMALL_START"
    
    //6.0 从redis中获取偏移量（程序启动时候，因为不在流中）
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)

    var recordDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffsetMap != null && kafkaOffsetMap.size>0) {
      for (o <- kafkaOffsetMap ) {
        println("reids:"+o._2)
      }
      recordDstream = KafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    } else {
      //2.0 从kafka中读取数据(如果第一次redis中没有kafka的偏移量，就从kafka初始位置读取)
      recordDstream = KafkaUtil.getKafkaStream(topic, ssc)
    }
    
    //7.0 从rdd中获取，offsetRange（分区号：结束时的偏移量）
    var offsetRanges: Array[OffsetRange] = Array.empty
    //将数据流转换为rdd
    val getOffsetDstream: DStream[ConsumerRecord[String, String]] = recordDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    
    //3.0 根据ts时间戳，提取日期和小时数
    val jsonObjDStream: DStream[JSONObject] = getOffsetDstream.map {
      record =>
        val v = record.value()
        //println(v)
        val jSONObject = JSON.parseObject(v)
        val ts = jSONObject.getLong("ts")
        val formate_ts = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        val splits = formate_ts.split(" ")
        jSONObject.put("dt", splits(0))
        jSONObject.put("hr", splits(1))
        jSONObject
    }

    //4.0 利用redis去重
    val fileterDstream = jsonObjDStream.mapPartitions { iter =>
      val jedis = RedisUtil.getJedisClient
      // jedis.set("dautest","hello world")
      val list = new ListBuffer[JSONObject]()
      val iter_list = iter.toList
      //println("过滤前：" + iter_list.size)
      for (obj <- iter_list) {
        val dt = obj.getString("dt")
        val mid = obj.getJSONObject("common").getString("mid")
        val dauKey = "dau:" + dt
        //1：未保存过，0：保存过
        val isNew = jedis.sadd(dauKey, mid)
        jedis.expire(dauKey, 3600 * 24)
        if (isNew == 1L) list += obj
      }
      jedis.close()
      //println("过滤后：" + list.size)
      list.toIterator
    }
    //fileterDstream.print(100)
    //5.0 封装样例类，批量插入ES中
    fileterDstream.foreachRDD { rdd =>
      rdd.foreachPartition { iter =>
        val list = iter.toList
        val dauList: List[(String, DauBean)] = list.map { jsonObj =>
          val commonJSONObj = jsonObj.getJSONObject("common")
          val mid = commonJSONObj.getString("mid")
          (mid, DauBean(mid,
            commonJSONObj.getString("uid"),
            commonJSONObj.getString("ar"),
            commonJSONObj.getString("ch"),
            commonJSONObj.getString("vc"),
            jsonObj.getString("dt"),
            jsonObj.getString("hr"),
            jsonObj.getLong("ts")
          ))
        }
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        ESUtil.insertBulk("gmall_dau_info_" + dateStr, dauList)
      }
      //提交（批次）偏移量到redis
      OffsetManager.submitOffset(topic, groupId, offsetRanges)
    }
    
    
    ssc.start()
    ssc.awaitTermination()
  }
}
