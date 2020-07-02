package com.bigdata.analyze.test

import com.bigdata.analyze.SortKey
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * @ author spencer
  * @ date 2020/5/28 10:30
  */
object TestApp2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TestApp2").setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    spark.udf.register("get_json_field", (jsonStr: String, field: String) => {
      val jSONObject = JSONObject.fromObject(jsonStr)
      jSONObject.getString(field)
    })

    spark.udf.register("concat_long_str", (v1: String, v2: Long, split: String) => {
      v1 + split + v2
    })

    spark.sql("select concat_long_str('hello', 100, '=') concat_str").show()

    spark.sql("select *, if (get_json_field(extend_info, 'product_status') = '0', '自营', '第三方') product_status from product_info").show(truncate = false)

    spark.sql("select product_id, product_name, if(get_json_field(extend_info, 'product_status') = '0', '0', 1) product_status from product_info").show(truncate = false)
//    val dataArray = Array((SortKey(2412,2284,2430), 1), (SortKey(2143,2320,2366), 2), (SortKey(2404,2199,2529), 3), (SortKey(2143,2320,2500), 4))
//
//    val dataRDD = sc.makeRDD(dataArray)
//
//    dataRDD.sortByKey(false).foreach(println)
//
//    println("====================================")
//
//    dataRDD.sortByKey().foreach(println)

//    val array = Array((1, "张三", 20), (2, "李四", 30))
//    val dataRDD = spark.sparkContext.makeRDD(array)

//    spark.createDataFrame(dataRDD).show()

//    import spark.implicits._
//    dataRDD.toDF("id", "name", "age").createOrReplaceTempView("student")
//    val studentDF = spark.sql("select * from student")
//
//    val studentRDD = studentDF.rdd.map {
//      case row =>
//        val id = row.getAs[Int]("id")
//        val name = row.getAs[String]("name")
//        val age = row.getAs[Int]("age")
//        Student(id, name, age)
//    }
//    studentRDD.foreach(println)
//    studentDF.show()
//    val studentArray = array.map(data => {
//      val id = data._1
//      val name = data._2
//      val age = data._3
//
//      Student(id, name, age)
//    })
//
//    val studentRDD = spark.sparkContext.makeRDD(studentArray)
//    studentRDD.map(student => {
//      val name = student.name
//      val age = student.age
//      (name, age)
//    }).foreach(println)

  }

}

case class Student(id: Int, name: String, age: Int)
