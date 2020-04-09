package com.bigdata.analyze.test

import java.util

import com.bigdata.commons.bean
import com.bigdata.commons.conf.ConfigurationManager
import com.bigdata.commons.constant.MyConstant
import com.bigdata.commons.dao.AdBlackListDAO
import com.bigdata.commons.dao.impl.AdBlacklistDAOImpl
import com.bigdata.commons.model.AdBlacklist
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TestAPP {

  case class Person(id: Double, name: String, age: Double, addr: String, job: String)

  def getPersonDF(spark: SparkSession) = {
    /**
      * spark读取hive数据转换为rdd
      */
    val personDF = spark.sql("select * from person")

    import spark.implicits._
    val personRDD = personDF.as[Person].rdd

    val personAddrRDD = personRDD.map(item => (item.id, item))
    val personAddrGroupRDD = personAddrRDD.groupByKey()
    personAddrGroupRDD.foreach(println)

    // personAddrGroupRDD: RDD[(id, iterable[Person])]
    val personAddr2RDD = personAddrGroupRDD.map {
      case (id, iterablePersonRDD) =>
        var id = -1.0
        var name: String = null
        var age = -1.0
        var addr: String = null
        var job: String = null

        val rows = ArrayBuffer[Person]()
        for (person <- iterablePersonRDD) {
          id = person.id
          name = person.name
          age = person.age
          addr = person.addr
          job = person.job

          rows += Person(id, name, age, addr, job)
        }

        (addr, rows)
    }
    personAddr2RDD.foreach(println)
  }

  /**
    * foldLeft测试，常用于累加器
    */
  def foldLeftTest(): Unit = {
    val wordsMap = Map("apple" -> 20, "pear" -> 10, "pineapple" -> 25, "grape" -> 30)

    // foldLeft(Map.empty[Int, Int]): 设定初始值为map(0, 0)
    val mergeMap = wordsMap.foldLeft(Map.empty[Int, Int]) {
      case (newMap, (fruit, count)) =>
        newMap + (fruit.length -> (newMap.getOrElse(fruit.length, 0) + count))
    }
    mergeMap.foreach(println)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TestAPP").setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
//
//    val sc = spark.sparkContext

//    val adBlacklistDAOImpl = new AdBlacklistDAOImpl()
//
//    val blacklists = adBlacklistDAOImpl.findAll().toArray()
//    blacklists.foreach(println)
//    blacklists.foreach()
    
//    println(blacklists)
//    val iterators = blacklists.iterator()
//
//    while (iterators.hasNext){
//      val blacklist = iterators.next()
//      println(blacklist)
//      blacklist
//    }

//    val jsonStr = ConfigurationManager.config.getString(MyConstant.TASK_PARAMS)
//    val taskParam = JSONObject.fromObject(jsonStr)
//    val startDate = taskParam.getString("startDate")
//    val targetPageFlow = taskParam.getString("targetPageFlow")
//
//    println(startDate)
//    println(targetPageFlow)

//    getPersonDF(spark)

//    var map1 = Map("zhangsan" -> 1)
//    var map2 = Map("lisi" -> 2)
//
//    map1.foldLeft(map2){
//      case (map, (k, v)) =>
////        map += (k -> (map.getOrElse(k, 0) + v))
//      map += (k -> (map.getOrElse(k, 0) + v))
//    }
//
//    val map3 = Map("成都" -> 1)
//
//    map1 ++= map2

//    println(map1)

//    val countMap = new mutable.HashMap[String, Int]()
//
//    countMap ++= map3
//
//    if (!countMap.contains("北京")){
//      countMap += ("北京" -> 1)
//    }
//    countMap.update("成都", countMap("成都") + 1)
//    println(countMap)

//    val countMap = new mutable.HashMap[String, Int]()
//    val map1 = Map("a" -> 1, "b" -> 2)

//    val map2 = Map("b" -> 4, "c" -> 8)
//
//    val mergeMap = map1.foldLeft(map2) {
//      case (map, (k, v)) =>
//        map + (k -> (map.getOrElse(k, 0) + v))
//    }
//
//    println(mergeMap)


//    foldLeftTest()

    val map1 = Map("a" -> 1, "b" -> 2)

    val map2 = Map("b" -> 4, "c" -> 8)

    val mergeMap = (map1 /: map2){
      case (newMap, (k ,v)) =>
        newMap + (k -> (newMap.getOrElse(k, 0) + v))
    }

    println(mergeMap)
  }


}
