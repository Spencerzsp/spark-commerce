package com.bigdata.analyze.test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * @ author spencer
  * @ date 2020/6/22 15:13
  */
object TransformationAndActionDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TransformationAndActionDemo").setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val rdd = spark.sparkContext.makeRDD(1 to 10, 2)

    //map算子
    val mapRDD = rdd.map(_ * 2)
    mapRDD.collect.foreach(println)

    //mapPartitions算子，对每个分区进行操作，效率比map更高，但可能OOM
    val mapPartitionsRDD = rdd.mapPartitions(datas => datas)
    mapPartitionsRDD.collect.foreach(println)

    //
    val mapPartitionsWithIndexRDD = rdd.mapPartitionsWithIndex {
      case (index, datas) =>
        datas.map(data => ("分区号：" + index, data))
    }
    mapPartitionsWithIndexRDD.collect.foreach(println)

  }


}
