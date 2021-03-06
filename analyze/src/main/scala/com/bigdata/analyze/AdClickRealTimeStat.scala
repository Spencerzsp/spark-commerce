package com.bigdata.analyze

import java.util
import java.util.Date

import com.bigdata.commons.bean.{AdBlacklist, AdUserClickCount}
import com.bigdata.commons.conf.ConfigurationManager
import com.bigdata.commons.constant.MyConstant
import com.bigdata.commons.impl.{AdBlacklistDAOImpl, AdUserClickCountDAOImpl}
import com.bigdata.commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConverters._

object AdClickRealTimeStat {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("AdClickRealTimeStat").setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("./streaming_checkpoint")

    // 获取kafka配置
    val kafkaBrokers = ConfigurationManager.config.getString(MyConstant.KAFKA_BROKERS)
    val kafkaTopics = ConfigurationManager.config.getString(MyConstant.KAFKA_TOPICS)

    // kafka消费者参数配置
    val kafkaParam = Map(
      "bootstrap.servers" -> kafkaBrokers, // 用于初始化连接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "commerce-consumer-group", // 用于标识这个消费者属于哪个消费团体(组)
      "auto.offset.reset" -> "latest", // 如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性，latest 表示自动重置偏移量为最新的偏移量
      "enable.auto.commit" -> (false: java.lang.Boolean) // 如果是 true，则这个消费者的偏移量会在后台自动提交
    )

    // adRealTimeLogDStream: DStream[RDD, RDD, ...] -> RDD[Message] -> Message[String, String]
    val adRealTimeLogDStream  = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafkaTopics), kafkaParam)
    )

    val adRealTimeValueDStream  = adRealTimeLogDStream.map(item => (item.value()))

    val adRealTimeFilterDStream = adRealTimeValueDStream.transform {
      //transform操作的是rdd数据集，map算子操作的是rdd中的具体元素
      consumerRecordRDD =>

        // 首先从mysql中查询所有黑名单用户
        val adBlacklistDAOImpl = new AdBlacklistDAOImpl()
        val adBlacklists = adBlacklistDAOImpl.findAll()

        import scala.collection.JavaConverters._
        adBlacklists.asScala.foreach(println)

        val userIdArray = adBlacklists.asScala.map(item => item.getUserid)

        consumerRecordRDD.filter {
          case consumerRecord =>
            val consumerRecordSplit = consumerRecord.split(" ")
            val userId = consumerRecordSplit(3)
            !userIdArray.contains(userId)
        }
    }

    //====================广告点击黑名单实时统计================================
    generateBlackListStat(adRealTimeFilterDStream)

//    adRealTimeFilterDStream.foreachRDD(rdd => rdd.foreach(println))

    val key2NumDStream  = adRealTimeFilterDStream.map {
      case consumerRedordRdd =>
        val consumerSplits = consumerRedordRdd.split(" ")
        val timestamp = consumerSplits(0).toLong
        val dateKey = DateUtils.formatDateKey(new Date(timestamp))
        val province = consumerSplits(1)
        val city = consumerSplits(2)
        val adid = consumerSplits(4)

        val key = dateKey + "_" + province + "_" + city + "_" + adid

        (key, 1L)
    }

    // 实时统计updateStateByKey
    val key2StateDStream  = key2NumDStream.updateStateByKey {
      (values: Seq[Long], state: Option[Long]) =>
        var newValue = 0L
//        if (state.isDefined) {
//          newValue = state.get
//        }
//        for (value <- values) {
//          newValue += value
//        }
        state match {
          case Some(count) =>
            for (value <- values) {
              newValue += value + count
            }
          case None =>
            for (value <- values) {
              newValue += value
            }
        }
        Some(newValue)
    }

    key2StateDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        item => item.foreach(println)
      }
    }

    ssc.start()
    ssc.awaitTermination()


  }

  /**
    * 广告点击黑名单实时统计
    */
  def generateBlackListStat(adRealTimeFilterDStream: DStream[String]) = {

    val key2NumDStream = adRealTimeFilterDStream.map {
      case consumerRecordRDD =>
        val consumerRecordSplit = consumerRecordRDD.split(" ")
        val timestamp = consumerRecordSplit(0).toLong
        val dateKey = DateUtils.formatDateKey(new Date(timestamp))
        val userid = consumerRecordSplit(3).toLong
        val adid = consumerRecordSplit(4).toLong

        val key = dateKey + "_" + userid + "_" + adid // 组合 key

        (key, 1L)
    }
    val key2CountDStream  = key2NumDStream.reduceByKey(_ + _)
    key2CountDStream.foreachRDD(rdd => rdd.foreach(println))

    val adUserClickCountDAOImpl = new AdUserClickCountDAOImpl()

    // 根据每一个 RDD 里面的数据，更新用户点击次数表数据
    key2CountDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val clickCountArray  = new util.ArrayList[AdUserClickCount]()
          val adUserClickCount = new AdUserClickCount()

          for ((key, count) <- items) {
            val keySplit = key.split("_")
            val date = keySplit(0)
            val userid = keySplit(1).toLong
            val adid = keySplit(2).toLong

            adUserClickCount.setDate(date)
            adUserClickCount.setUserid(userid)
            adUserClickCount.setAdid(adid)
            adUserClickCount.setClickCount(count)

            clickCountArray.add(adUserClickCount)
          }

          //更新用户点击次数
          adUserClickCountDAOImpl.updateBatch(clickCountArray)

      }
    }

    //动态更新黑名单表数据
    // 对 DStream 做 filter 操作：就是遍历 DStream 中的每一个 RDD 中的每一条数据
    // key2BlackListDStream: DStream[RDD, RDD, RDD, ...] -> RDD[(key, 150)]
    val key2BlackListDStream = key2CountDStream.filter {
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val userid = keySplit(1).toLong
        val adid = keySplit(1).toLong

        val clickCount = adUserClickCountDAOImpl.findClickCountByMultiKey(date, userid, adid)
        if (clickCount > 100)
          true
        else
          false
    }

    // key2BlackListDStream.map: DStream[RDD[userid]]
    val userIdDStream = key2BlackListDStream.map {
      case (key, count) =>
        key.split("_")(1).toLong
    }.transform(rdd => rdd.distinct())

    // 将结果批量插入mysql数据库中
    userIdDStream.foreachRDD(
      rdd => rdd.foreachPartition{
        items =>
          val adBlacklists = new util.ArrayList[AdBlacklist]()
          val adBlacklist = new AdBlacklist()
          for (userId <- items) {
            adBlacklist.setUserid(userId)
            adBlacklists.add(adBlacklist)
          }
          val adBlacklistDAOImpl = new AdBlacklistDAOImpl()
          adBlacklistDAOImpl.insertBatch(adBlacklists)
      })

  }

}
