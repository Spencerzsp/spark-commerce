package com.bigdata.analyze.function

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Row对象转换成RDD和DF
  * 样例类与DF和RDD的转换
  * @ author spencer
  * @ date 2020/7/2 16:54
  */

object SparkRowDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkRowDemo").setMaster("local[*]")


    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val userRDD = spark.sparkContext.makeRDD(Seq(
      (1, "张三", 20, "成都"),
      (2, "李四", 30, "北京"),
      (3, "王五", 40, "上海")
    ))

//    val userDF: DataFrame = spark.createDataFrame(userRDD)
//
//    val userMidRDD: RDD[User] = userDF.rdd.flatMap(rdd => {
//      val rows = ArrayBuffer[User]()
//      val id = rdd.getInt(0)
//      val name = rdd.getString(1)
//      val age = rdd.getInt(2)
//      val addr = rdd.getString(3)
//
//      rows += User(id, name, age, addr)
//    })
//
//    spark.createDataFrame(userMidRDD).show(truncate = false)

    val userSchemaRDD = userRDD.map(rdd => {

      val id = rdd._1
      val name = rdd._2
      val age = rdd._3
      val addr = rdd._4

      User(id, name, age, addr)
    })

//    import spark.implicits._
//    userSchemaRDD.toDF().show()

    spark.createDataFrame(userSchemaRDD).show()
  }
}
