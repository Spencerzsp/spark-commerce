package com.bigdata.analyze.test

import java.text.SimpleDateFormat
import java.util.UUID

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @ author spencer
  * @ date 2020/4/29 13:58
  */
object Spark2HBase {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark2HBase").setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
    val startTime = System.currentTimeMillis()
    println(format.format(startTime))

    val path = getClass.getResource("/user_behavior.csv").getPath
    val dataSetRDD = spark.sparkContext.textFile(path).map(_.split(","))
    dataSetRDD.foreachPartition(data => {
      val config = HBaseConfiguration.create()
      val tableName = TableName.valueOf("kafka-flink-hbase")

      val hTable = new HTable(config, tableName)
      hTable.setAutoFlush(false)
      hTable.setWriteBufferSize(3*1024*1024)

      data.foreach(dataArray => {
        val userId = dataArray(0).trim
        val itemId = dataArray(1).trim
        val categoryId = dataArray(2).trim
        val behavior = dataArray(3).trim
        val timestamp = dataArray(4).trim

        val cf = "cf"
        val rowKey = UUID.randomUUID().toString.replaceAll("-", "").substring(0,10) + "_" + timestamp

        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("userId"), Bytes.toBytes(userId))
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("itemId"), Bytes.toBytes(itemId))
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("categoryId"), Bytes.toBytes(categoryId))
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("behavior"), Bytes.toBytes(behavior))
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("timestamp"), Bytes.toBytes(timestamp))

        hTable.put(put)
      })
      hTable.flushCommits()
    })

    val endTime = System.currentTimeMillis()
    println(format.format(endTime))
  }
}
