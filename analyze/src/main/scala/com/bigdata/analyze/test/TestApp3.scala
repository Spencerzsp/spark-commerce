package com.bigdata.analyze.test

import java.util

import com.bigdata.commons.bean.AdBlacklist
import com.bigdata.commons.impl.AdBlacklistDAOImpl
import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.sql.SparkSession

/**
  * @ author spencer
  * @ date 2020/6/2 9:54
  * 自定义分区的使用
  */
object TestApp3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TestApp3").setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val rdd = spark.sparkContext.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    rdd.partitionBy(new MyPartitioner(3)).saveAsTextFile("D:\\IdeaProjects\\spark-commerce\\output")

  }
}

//自定义分区器
class MyPartitioner(partitions: Int) extends Partitioner{

  override def numPartitions = partitions

  override def getPartition(key: Any) = {

    if (key.equals("a")){
      0 //0号分区
    }else if (key.equals("b")){
      1
    }else{
      2
    }

  }
}
