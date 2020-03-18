package com.bigdata.commons.conf

import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters

/**
  * 配置工具类，用于读取配置文件
  */
object ConfigurationManager {
  // 创建用于初始化配置生成器实例的参数对象
  private val params = new Parameters()

  // FileBasedConfigurationBuilder : 产生一个传入的类的实例对象
  // FileBasedConfiguration : 融合 FileBased 与 Configuration 的接口
  // PropertiesConfiguration : 从一个或者多个文件读取配置的标准配置加载器
  // configure() : 通过 params 实例初始化配置生成器
  // 向 FileBasedConfigurationBuilder() 中传入一个标准配置加载器类，生成一个加载器类的实例对象，然后通过 params 参数对其初始化
  private val builder = new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
    .configure(params.properties().setFileName("commerce.properties"))

  // 通过 getConfiguration 获取配置对象
  val config = builder.getConfiguration()
}
