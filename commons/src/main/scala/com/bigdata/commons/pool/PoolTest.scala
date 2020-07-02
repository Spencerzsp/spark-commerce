package com.bigdata.commons.pool
import java.sql.ResultSet
import java.text.SimpleDateFormat

import org.joda.time.DateTime

/**对象池的使用
  * @ author spencer
  * @ date 2020/5/8 13:39
  */
object PoolTest {
  def main(args: Array[String]): Unit = {
//    val mySqlProxy = CreateMySqlPool.apply().borrowObject()
//    val sql = "select * from person"
//    mySqlProxy.executeQuery(sql, null, new QueryCallback {
//      override def process(rs: ResultSet): Unit = {
//        while (rs.next()){
//          println(rs.getInt(1) + "\t" + rs.getString(2) + "\t" + rs.getInt(3) + "\t" + rs.getString(4))
//        }
//      }
//    })
//
////    mySqlProxy.shutdown()
//    CreateMySqlPool.apply().returnObject(mySqlProxy)

    println(DateTime.now().toString("yyyy-MM-dd HH:mm:ss"))
  }
}
