package com.bigdata.analyze.function

import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @ author spencer
  * @ date 2020/7/2 10:19
  */
//case class User(id: Long, name: String, age: Long, addr: String)
object SparkSQLDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkSQLDemo").setMaster("local[*]")


    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val userDF = spark.read.csv("D:\\IdeaProjects\\spark-commerce\\analyze\\src\\main\\resources\\user.csv")
      .toDF("id", "name", "age", "addr")

    userDF.createOrReplaceTempView("user")

    //使用UDTF
    spark.udf.register("user_avg", SparkUDAFDemo)

    val sql1 = "select addr, user_avg(age) avg_age from user group by addr"
    spark.sql(sql1).show()

    //使用UDF
    spark.udf.register("get_json_field", (json: String, field: String) => {
      val jSONObject = JSONObject.fromObject(json)
      jSONObject.getString(field)
    })

//    val sql2 = "select get_json_field(extend_info, product_status) product_status from product_info"
//    spark.sql(sql2).show()

    spark.sql("CREATE temporary FUNCTION user_udtf as 'com.bigdata.analyze.function.SparkUDTFDemo'")

    val sql3 = "select id, name, age, user_udtf(addr) from user"

    spark.sqlContext.sql(sql3).show()

//    spark.sql("show functions").show(1000, truncate = false)

  }
}
