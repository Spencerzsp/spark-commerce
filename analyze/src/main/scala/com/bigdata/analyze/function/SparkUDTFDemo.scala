package com.bigdata.analyze.function

import java.util

import org.apache.hadoop.hive.ql.exec.{UDFArgumentException, UDFArgumentLengthException}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory}

/**
  * @ author spencer
  * @ date 2020/7/2 12:00
  */
object SparkUDTFDemo extends GenericUDTF{

  /**
    * 初始化设置输出字段名称和类型
    * @param args
    * @return
    */
  override def initialize(args: Array[ObjectInspector]) = {

    //输入参数校验
    if (args.length != 1) {
      throw new UDFArgumentLengthException("UserDefinedUDTF takes only one argument")
    }
    if (args(0).getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentException("UserDefinedUDTF takes string as a parameter")
    }
    val fieldName = new util.ArrayList[String]()
    val fieldOIs = new util.ArrayList[ObjectInspector]

    //定义输出列默认字段名称
    fieldName.add("col1")

    //定义输出字段类型
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)

    ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOIs)
  }

  /**
    * 处理数据(每次处理一行)
    * @param objects
    */
  override def process(objects: Array[AnyRef]) = {

    val input = objects(0).toString

    val strList = input.split("")

    val result = new Array[String](1)
    for (str <- strList) {

      result(0) = str
      forward(result)
    }
  }

  override def close() = ???
}
