package com.bigdata.analyze.function

import com.bigdata.analyze.GroupConcatDistinct
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator


/**
  * @ author spencer
  * @ date 2020/7/14 13:46
  */

case class Employee(name: String, salary: Long)
case class Average(var sum: Long, var count: Long)

object SparkUDAFDemo02{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkSQLDemo").setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val employeeRDD = spark.read.json("file:///D:\\spark-2.3.0-bin-hadoop2.7\\examples\\src\\main\\resources\\employees.json")

    import spark.implicits._
    val employeeDF = employeeRDD.as[Employee]

    //第一种方式调用udaf
    val averageSalary = MyUDAF.toColumn.name("average_salary")
    employeeDF.select(averageSalary).show()

    //第二种方式调用udaf
    employeeDF.createOrReplaceTempView("employee")

//    import org.apache.spark.sql.functions
//    spark.udf.register("averageSalary", functions.u)
//    spark.sql("select averageSalary(salary) from employee").show()

    spark.stop()

  }
}

object MyUDAF extends Aggregator[Employee, Average, Double]{
  override def zero = Average(0L, 0L)

  override def reduce(buffer: Average, employee: Employee) = {
    buffer.sum += employee.salary
    buffer.count += 1

    buffer
  }

  override def merge(buffer1: Average, buffer2: Average) = {
    buffer1.sum += buffer2.sum
    buffer1.count += buffer2.count

    buffer1
  }

  override def finish(reduction: Average) = reduction.sum / reduction.count

  override def bufferEncoder = Encoders.product

  override def outputEncoder = Encoders.scalaDouble
}
