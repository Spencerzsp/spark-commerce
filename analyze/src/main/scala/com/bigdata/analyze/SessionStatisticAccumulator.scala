package com.bigdata.analyze

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class SessionStatisticAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]](){
  // 自定义累加器：要求要在类的里面维护一个 mutable.HashMap 结构
  val countMap = new mutable.HashMap[String, Int]()

  // 判断累加器是否为空
  override def isZero: Boolean = {
    this.countMap.isEmpty
  }

  // 复制一个一模一样的累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val acc = new SessionStatisticAccumulator

    // 将两个 Map 拼接在一起
    // map1 = Map(1 -> "zhangsan"), map2 = Map(2 -> "lisi")
    // Map(1 -> zhangsan, 2 -> lisi)
    acc.countMap ++= this.countMap
    acc
  }

  // 重置累加器
  override def reset(): Unit = {
    this.countMap.clear()
  }

  // 向累加器中添加 KV 对（K 存在，V 累加1，K 不存在，重新创建）
  override def add(k: String): Unit = {
    //如果K不存在，添加一个键值对到countMap中，初始值value为0
    if (!this.countMap.contains(k)) {
      this.countMap += (k -> 0)
    }

    //对键值对进行更新
    this.countMap.update(k, this.countMap(k) + 1)

  }

  // 两个累加器进行合并（先判断两个累加器是否是同一类型的，再将两个 Map 进行合并(是个小难点)）
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      // (1 to 100).foldLeft(0) 等价于 (0 /: (1 to 100))(_+_)  又等价于 { case (int1, int2) => int1 + int2 }
      // acc.countMap.foldLeft(this.countMap) 等价于 this.countMap /: acc.countMap  又等价于 this.countMap 和 acc.countMap 的每一个 KV 做操作
      // 合并两个map，对相同key的value进行累加,返回值为newMap
      case acc: SessionStatisticAccumulator => acc.countMap.foldLeft(this.countMap) {
        case (newMap, (k, v)) =>
          newMap += (k -> (newMap.getOrElse(k, 0) + v))
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.countMap
  }
}
