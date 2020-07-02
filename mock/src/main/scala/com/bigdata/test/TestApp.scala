package com.bigdata.test

import java.util.UUID

import com.bigdata.commons.model.{UserInfo, UserVisitAction}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * @ author spencer
  * @ date 2020/6/22 11:58
  */
object TestApp {

  def mockUserVistAction(spark: SparkSession) = {
    val rows = ArrayBuffer[UserVisitAction]()
    val actions = Array("click", "search", "order", "pay")
    val searchKeywords = Array("华为手机", "联想笔记本", "小龙虾", "卫生纸", "吸尘器", "Lamer", "机器学习", "苹果", "洗面奶", "保温杯")
    val random = new Random()

    for (i <- 0 until 100) {
      val action = actions(random.nextInt(4))
      action match {
        case "click" =>
          println("click")
        case "search" =>
          println(searchKeywords(random.nextInt(10)))
        case "order" =>
          println("order")
        case _ =>
          println("pay")
      }
    }

//    for (i <- 0 until 100){
//
//      println(random.nextInt(3))
//    }

  }

  def mockUserInfo() = {
    val rows = ArrayBuffer[UserInfo]()
    val random = new Random()
    val sexs = Array("male", "female")

    for (i <- 0 until 100){
      val userId = i
      val userName = "user_" + i
      val age = random.nextInt(70)
      val professional = "professional_" + random.nextInt(100)
      val sex = sexs(random.nextInt(2))

//      rows += UserInfo()
    }
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("TestApp")
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    mockUserVistAction(spark)
    mockUserInfo()
  }

}
