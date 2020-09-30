package com.bigdata.analyze.function

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * 实现的逻辑：由1条记录生成10条记录
  * 使用 FlatMap 算子对Row 进行膨胀
  * @ author spencer
  * @ date 2020/7/2 14:30
  */

case class User(id: Int, name: String, age: Int, addr: String)

object SparkUDTFDemo2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkUDTFDemo2").setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val userRDD: RDD[User] = spark.sparkContext.makeRDD(Seq(
        User(1, "盖伦", 20, "德玛西亚|班德尔城|黑色玫瑰"),
        User(2, "赵信", 25, "艾欧尼亚|诺克萨斯"),
        User(3, "皇子", 30, "德玛西亚")
      )
    )

    val userDF = spark.createDataFrame(userRDD)

    // userDF.rdd返回类型为RDD[Row]，可通过StructType重新赋予字段和类型
    val rdd: RDD[Row] = userDF.rdd

    import spark.implicits._
    // userDF.as[User].rdd返回类型为RDD[User]
    val rdd1: RDD[User] = userDF.as[User].rdd

    val midRDD = userDF.rdd.flatMap {
      user => {
        val x = ArrayBuffer[Row]()

        val strs = user.getString(3).split("\\|")

        for (str <- strs) {
          x.+=:(Row(user.getInt(0), user.getString(1), user.getInt(2), str))
        }
        x
      }
    }

    println(midRDD.count())

    val finalDF = spark.createDataFrame(midRDD, StructType(
      Array(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("addr-split", StringType)
      )
    ))

    finalDF.show(truncate = false)
  }

}
