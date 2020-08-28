package com.duoduo.realtime.utils

import java.io
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
 * Author z
 * Date 2020-08-27 10:02:21
 */
object KafkaUtil {
  private val properties: Properties = PropertiesUtil.load("config.properties")
  private val broker_list = properties.getProperty("kafka.broker.list")
  var kafkaProducer: KafkaProducer[String, String] = null
  
   var kafkaParam: mutable.Map[String, io.Serializable] = collection.mutable.Map(
    "bootstrap.servers" -> broker_list, //用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "gmall_consumer_group",
    //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    //可以使用这个配置，latest自动重置偏移量为最新的偏移量//earliest/latest
    "auto.offset.reset" -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  // 创建DStream，返回接收到的输入数据
  // LocationStrategies：根据给定的主题和集群地址创建consumer
  // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
  // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
  // ConsumerStrategies.Subscribe：订阅一系列主题
  def getKafkaStream(topic: String, ssc: StreamingContext)
  : InputDStream[ConsumerRecord[String, String]] = {
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils
      .createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
      )
    dStream
  }
  
  def getKafkaStream(topic: String, ssc: StreamingContext, groupid: String)
  : InputDStream[ConsumerRecord[String, String]] = {
    kafkaParam("group.id") = groupid
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils
      .createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
      )
    dStream
  }
  
  def getKafkaStream(topic: String,
                     ssc: StreamingContext,
                     offsets: Map[TopicPartition, Long]
                     , groupid: String)
  : InputDStream[ConsumerRecord[String, String]] = {
    kafkaParam("group.id") = groupid
    val dStream: ConsumerStrategy[String, String] = ConsumerStrategies
      .Subscribe[String, String](Array(topic), kafkaParam, offsets)
    for (o <- offsets ) {
      println("kafka加载开始位置："+o._2)
    }
 
    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      dStream
    )
  }
  
  /**
   * 创建producer
   * @return
   */
  private  def createKafkaProducer: KafkaProducer[String, String] ={
    import scala.collection.JavaConversions._
    //​ 如何实现幂等性：ack=-1 并且将 Producer 的参数中 enable.idompotence 设置为 true
    kafkaParam.put("enable.idompotence",(true: java.lang.Boolean))
    kafkaParam.remove("group.id")
    kafkaParam.remove("enable.auto.commit")
    kafkaParam.remove("auto.offset.reset")
    var producer:KafkaProducer[String,String]=null
    try
      producer=new KafkaProducer[String,String](kafkaParam)
    catch {
      case e:Exception=>
        e.printStackTrace()
    }
    producer
  }
  
  /**
   * 向Kafka发送信息
   * @param topic
   * @param msg
   * @return
   */
  def send(topic:String,msg:String)={
    if(kafkaProducer==null) kafkaProducer=createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String,String](topic,msg))
  }
  def send(topic:String,key:String,msg:String)={
    if(kafkaProducer==null) kafkaProducer=createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String,String](topic,key,msg))
  }
  def main(args: Array[String]): Unit = {

  }
}
