package com.bigdata.mock

import java.util.Properties

import com.bigdata.commons.conf.ConfigurationManager
import com.bigdata.commons.constant.MyConstant
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object MockRealTimeData {

  def createKafkaProducer(brokers: String): KafkaProducer[String, String] = {

    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, String](prop)
  }

  def generateMockDate() = {

    val array = ArrayBuffer[String]()

    val random = new Random()

    for (i <- 0 until 50) {
      val timestamp = System.currentTimeMillis()
      val province = random.nextInt(10)
      val city = province
      val adid = random.nextInt(20)
      val userid = random.nextInt(100)

      // 拼接实时数据
      array += timestamp + " " + province + " " + city + " " + userid + " " + adid
    }

    array.toArray
  }

  def main(args: Array[String]): Unit = {

    val brokers = ConfigurationManager.config.getString(MyConstant.KAFKA_BROKERS)
    val topics = ConfigurationManager.config.getString(MyConstant.KAFKA_TOPICS)

    val kafkaProducer = createKafkaProducer(brokers)

    while (true){
      for (item <- generateMockDate()) {
        kafkaProducer.send(new ProducerRecord[String, String](topics, item))
        println("正在发送数据到kafka集群...")
      }

      Thread.sleep(5000)
    }
  }

}
