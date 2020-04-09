package com.bigdata.analyze.test

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class MyAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]]{

  val countMap = mutable.HashMap[String, Int]()

  override def isZero: Boolean = ???

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = ???

  override def reset(): Unit = ???

  override def add(v: String): Unit = ???

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]) = {
    other match {
      case acc: MyAccumulator => acc.countMap.foldLeft(this.countMap){
        case (newMap, (k ,v)) =>
          newMap += (k -> (newMap.getOrElse(k, 0) + v))
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = ???
}
