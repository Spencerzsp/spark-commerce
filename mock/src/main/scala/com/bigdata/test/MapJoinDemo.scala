package com.bigdata.test

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @ description: 
  * @ author: spencer
  * @ date: 2020/9/1 11:38
  */
object MapJoinDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MapJoinDemo").setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd1: RDD[(Int, Int)] = sc.makeRDD(List(30,40,50,60,70)).map((_, 1))
    val rdd2: RDD[(Int, String)] = sc.makeRDD(List(30,40,50,60,70)).map((_, "a"))

    //使用join会出现shuffle
//    rdd1.join(rdd2).collect.foreach(println)

    //使用map join避免shuffle
    //将其中一个较小的rdd进行广播到所有的executors
    val bdRDD2: Broadcast[Array[(Int, String)]] = sc.broadcast(rdd2.collect)
    val rdd3: RDD[(Int, (Int, String))] = rdd1.flatMap {
      case (k1, v1) => {
        val value: Array[(Int, String)] = bdRDD2.value
        value.filter(_._1 == k1).map {
          case (k2, v2) => {
            (k1, (v1, v2))
          }
        }
      }
    }
    rdd3.collect.foreach(println)

    sc.stop()
  }
}
