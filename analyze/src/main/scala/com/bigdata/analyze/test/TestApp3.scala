package com.bigdata.analyze.test

import java.util

import com.bigdata.commons.bean.AdBlacklist
import com.bigdata.commons.impl.AdBlacklistDAOImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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

//    val rdd = spark.sparkContext.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

//    rdd.partitionBy(new MyPartitioner(3)).saveAsTextFile("D:\\IdeaProjects\\spark-commerce\\output")

    //spark.sparkContext.textFile: RDD[String]
    val lines: RDD[String] = spark.sparkContext.textFile("D:\\IdeaProjects\\spark-commerce\\analyze\\src\\main\\resources\\word.txt")

    //spark.read.textFile: Dataset[String]
    val data: Dataset[String] = spark.read.textFile("D:\\IdeaProjects\\spark-commerce\\analyze\\src\\main\\resources\\README.md")

//    val data: Dataset[String] = spark.read.textFile("D:\\IdeaProjects\\spark-commerce\\analyze\\src\\main\\resources\\word.txt")

    import spark.implicits._
    val wordCounts: Dataset[(String, Long)] = data.flatMap(line => line.split(" ")).groupByKey(identity).count()

//    val wordCounts = data.flatMap(line => line.split(" ")).groupByKey(identity).count()

    wordCounts.collect().foreach(println)

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
