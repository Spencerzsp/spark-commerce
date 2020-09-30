package com.bigdata.test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * @ author spencer
  * @ date 2020/7/10 13:48
  */

case class Student(id: Int, name: String, age: Int)

object TestApp2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TestApp2").setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.shuffle.io.maxRetries", "60")
      .config("spark.shuffle.io.retryWait", "60s")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd1: RDD[(Int, Int)] = sc.makeRDD(List(30,40,50,60,70)).map((_, 1))
    val rdd2: RDD[(Int, String)] = sc.makeRDD(List(30,40,50,60,70)).map((_, "a"))

//    rdd1.map((_._2 + 1)).foreach(println)

    val words = List("hello,spark,hadoop","flink,spark,flink,spark")

    val rdd3: RDD[String] = sc.makeRDD(words)
    val mapRDD3: RDD[Array[String]] = rdd3.map(_.split(","))
    val flatMapRDD3: RDD[String] = rdd3.flatMap(_.split(","))
//    spark.existingSharedState.getOrElse("spark", "hello")

    mapRDD3.collect.foreach(println)
    flatMapRDD3.foreach(println)

//    val students = ArrayBuffer[Student]()
//
//    for (i <- 1 to 10) {
//      val id = i
//      val name = "spark_" + i
//      val random = new Random()
//      val age = random.nextInt(10) + 10
//
//      students += Student(id, name, age)
//    }
//
//    students.toArray
//
//    val studentsRDD = spark.sparkContext.makeRDD(students)
//
//    spark.createDataFrame(studentsRDD).show()

//    import spark.implicits._
//
//    studentsRDD.toDF().show()
//
//    studentsRDD.toDF().write.mode("append").format("jdbc")

    spark.stop()
  }
}
