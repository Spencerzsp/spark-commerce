package com.bigdata.commons.dao

import java.sql.ResultSet

import com.bigdata.commons.conf.ConfigurationManager
import com.bigdata.commons.constant.MyConstant
import com.bigdata.commons.model.AdBlacklist
import com.bigdata.commons.pool.{MySqlProxy, QueryCallback}

import scala.collection.mutable.ArrayBuffer

object AdBlackListDAO extends QueryCallback{

  val url: String = ConfigurationManager.config.getString(MyConstant.JDBC_URL)
  val user: String = ConfigurationManager.config.getString(MyConstant.JDBC_USER)
  val password: String = ConfigurationManager.config.getString(MyConstant.JDBC_PASSWORD)

  def findAll() = {

    val mySqlProxy = MySqlProxy.apply(url, user, password)
    println(mySqlProxy)
    val sql = "select * from ad_blacklist"
    println(sql)
//    val rs = mySqlProxy.executeQuery(sql, null,queryCallback)

//    process(rs)

  }

  override def process(rs: ResultSet) = {
    val rows = ArrayBuffer[AdBlacklist]()
    while (rs.next()){
      val userId = rs.getLong(1)
      rows += AdBlacklist(userId)
    }
    rows.toArray
  }
}
