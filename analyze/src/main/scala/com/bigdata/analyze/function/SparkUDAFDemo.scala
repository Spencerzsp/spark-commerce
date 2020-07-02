package com.bigdata.analyze.function

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 聚合函数求平均数
  * @ author spencer
  * @ date 2020/7/2 9:59
  */
object SparkUDAFDemo extends UserDefinedAggregateFunction{

  //输入数据类型
  override def inputSchema: StructType = StructType(StructField("input", LongType)::Nil)

  //缓冲区数据类型
  override def bufferSchema: StructType = StructType(StructField("sum", LongType)::StructField("count", LongType)::Nil)

  //聚合之后输出数据类型
  override def dataType: DataType = DoubleType

  //相同输入是否总能得到相同输出
  override def deterministic: Boolean = true

  //初始化缓冲区
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //给聚合函数传入一条数据进行处理
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1

    buffer.update(0, buffer(0))
    buffer.update(1, buffer(1))
  }

  //合并聚合函数缓冲区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算最终结果
  override def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble / buffer.getLong(1)
}
