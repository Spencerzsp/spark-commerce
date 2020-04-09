package com.bigdata.analyze.test

import java.util

import com.bigdata.commons.bean
import com.bigdata.commons.dao.AdBlackListDAO
import com.bigdata.commons.dao.impl.AdBlacklistDAOImpl
import com.bigdata.commons.model.AdBlacklist
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object TestAPP {

  def getBlackList(blacklists: util.List[bean.AdBlacklist]): Unit = ???

  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf().setAppName("TestAPP").setMaster("local[*]")
//    val spark = SparkSession.builder()
//      .config(conf)
//      .enableHiveSupport()
//      .getOrCreate()
//
//    val sc = spark.sparkContext

    val adBlacklistDAOImpl = new AdBlacklistDAOImpl()

    val blacklists = adBlacklistDAOImpl.findAll().toArray()
    blacklists.foreach(println)
//    blacklists.foreach()
    
//    println(blacklists)
//    val iterators = blacklists.iterator()
//
//    while (iterators.hasNext){
//      val blacklist = iterators.next()
//      println(blacklist)
//      blacklist
//    }
  }
  
  

}
