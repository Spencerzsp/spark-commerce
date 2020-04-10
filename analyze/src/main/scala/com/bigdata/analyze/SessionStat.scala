package com.bigdata.analyze

import java.util.{Date, UUID}

import com.bigdata.commons.conf.ConfigurationManager
import com.bigdata.commons.constant.MyConstant
import com.bigdata.commons.model._
import com.bigdata.commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object SessionStat {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SessionStat").setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    //读取配置文件中的json字符串
//    task.params.json={\
//      startDate:"2020-03-01", \
//      endDate:"2020-03-31", \
//      startAge: 20, \
//      endAge: 50, \
//      professionals: "",  \
//      cities: "", \
//      sex:"", \
//      keywords:"", \
//      categoryIds:"", \
//      targetPageFlow:"1,2,3,4,5,6,7"}
    val jsonStr = ConfigurationManager.config.getString(MyConstant.TASK_PARAMS)

    //将json字符串转换成json对象
    val taskParam = JSONObject.fromObject(jsonStr)

    // 创建全局唯一的主键，每次执行 main 函数都会生成一个独一无二的 taskUUID，来区分不同任务，作为写入 MySQL 数据库中那张表的主键
    val taskUUID = UUID.randomUUID().toString

    //userVisitActionRDD: RDD[UserVisitAction]
    val userVisitActionRDD = getActionRDD(spark, taskParam)

    //将用户行为转换为K-V结构，sessionId2ActionRDD: RDD[session_id, UserVisitAction]
    val sessionId2ActionRDD = userVisitActionRDD.map(item => (item.session_id, item))

//    sessionId2ActionRDD.foreach(println)

    //sessionId2ActionGroupRDD: RDD[session_id, Iterable[UserVisitAction]]
    val sessionId2ActionGroupRDD = sessionId2ActionRDD.groupByKey()

    sessionId2ActionGroupRDD.cache()

//    sessionId2ActionGroupRDD.foreach(println)

    //sessionId2FullAggrInfoRDD: RDD[session_id, fullAggrInfo]
    val sessionId2FullAggrInfoRDD = getSessionFullAggrInfo(spark, sessionId2ActionGroupRDD)

//    sessionId2FullAggrInfoRDD.foreach(println)

    // 创建自定义累加器对象
    val sessionStatisticAccumulator = new SessionStatisticAccumulator

    // 在 sparkSession 中注册自定义累加器，这样后面就可以用了
    sc.register(sessionStatisticAccumulator)

    //sessionId2FilterRDD: RDD[session_id, fullInfo]
    val sessionId2FilterRDD = getSessionFilterRDD(taskParam, sessionId2FullAggrInfoRDD, sessionStatisticAccumulator)

//    sessionId2FilterRDD.foreach(println)

//    getSessionRatio(spark, taskUUID, sessionStatisticAccumulator.value)

//    println("计算session占比完成~~~")

    // ******************** 需求二：Session 随机抽取 ********************
    sessionRandomExtract(spark, taskUUID, sessionId2FilterRDD)

//    println("计算session随机抽取完成")

    // ******************** 需求三：Top10 热门品类统计 ********************

    // sessionId2ActionRDD: RDD[(sessionId, UserVisitAction)]
    // seeionId2FilterRDD: RDD[(sessionId, fullAggrInfo)]

    // join 默认是内连接，即不符合条件的不显示（即被过滤掉）
    
    val sessionId2ActionFilterRDD  = sessionId2ActionRDD.join(sessionId2FilterRDD).map{
      case (sessionId, (userVisitAction, filterInfo)) =>
        (sessionId, userVisitAction)
    }

    top10PopularCategories(spark, taskUUID, sessionId2ActionFilterRDD)


    // ******************** 需求五：页面单跳转化率统计 ********************
    
    val actionRDD = getActionRDD(spark, taskParam)
//    val sessionId2ActionRDD = actionRDD.map(item => (item.session_id, item))

    sessionId2ActionRDD.persist(StorageLevel.MEMORY_ONLY)

//    val targetPageFlowStr = ConfigurationManager.config.getString(MyConstant.PARAM_TARGET_PAGE_FLOW)
    val targetPageFlowStr = ParamUtils.getParam(taskParam, MyConstant.PARAM_TARGET_PAGE_FLOW)
    val targetPageFlowArray = targetPageFlowStr.split(",")

    //获取限制条件的页面切片
    val targetPageSplit = targetPageFlowArray.slice(0, targetPageFlowArray.length - 1).zip(targetPageFlowArray.tail).map {
      case (page1, page2) =>
        (page1 + "_" + page2)
    }
//    targetPageSplit.foreach(println)

    val sessionId2GroupActionRDD = sessionId2ActionRDD.groupByKey()

    val realPageSplitNumRDD = sessionId2GroupActionRDD.flatMap {
      case (sessionId, iterableActionRDD) =>

        //将UserVisitActionRDD按照时间顺寻进行排序
        val sortList = iterableActionRDD.toList.sortWith((item1, item2) => {
          DateUtils.parseTime(item1.action_time).getTime <
            DateUtils.parseTime(item2.action_time).getTime
        })

        val pageList = sortList.map {
          case userVisitAction =>
            userVisitAction.page_id
        }

        val realPageList = pageList.slice(0, pageList.length - 1).zip(pageList.tail).map {
          case (page1, page2) =>
            (page1 + "_" + page2)
        }

        // 过滤：留下存在于 targetPageSplit 中的页面切片
        val realPageListFilter = realPageList.filter {
          case realPageList =>
            targetPageSplit.contains(realPageList)
        }

        realPageListFilter.map {
          case realPageListFilter =>
            (realPageListFilter, 1)
        }
    }

    //realPageSplitCountMap: Map[(page1_page2, count)]
    val realPageSplitCountMap = realPageSplitNumRDD.countByKey()

    realPageSplitCountMap.foreach(println)

    val startPage = targetPageFlowArray(0).toLong

    val startPageCount = sessionId2ActionRDD.filter {
      case (sessionId, userVisitAction) =>
        userVisitAction.page_id == startPage
    }.count()

    println(startPage)

//    getPageConvertRate(spark, taskUUID, targetPageSplit, startPageCount, realPageSplitCountMap)

    // ******************** 需求六：各区域 Top3 商品统计 ********************

    // cityId2ProductIdRDD: RDD[(cityId, productId)]
    val cityId2ProductIdRDD  = getCityAndProductInfo(spark, taskParam)
//    cityId2ProductIdRDD.foreach(println)

    // cityId2AreaInfoRDD: RDD[(cityId, cityName, area)]
    val cityId2AreaInfoRDD  = getCityAreaInfo(spark)
    cityId2AreaInfoRDD.foreach(println)

    val AreaProductIdBasicInfoDF = getAreaProductIdBasicInfoTable(spark, cityId2ProductIdRDD, cityId2AreaInfoRDD)
    AreaProductIdBasicInfoDF.show(truncate = false)

  }

  def getAreaProductIdBasicInfoTable(spark: SparkSession,
                                     cityId2ProductIdRDD: RDD[(Long, Long)],
                                     cityId2AreaInfoRDD: RDD[(Long, CityAreaInfo)]) = {

    val areaProductIdBasicInfoRDD  = cityId2ProductIdRDD.join(cityId2AreaInfoRDD).map{
      case (cityId, (clickProductId, cityAreaInfo)) =>
        (cityId, cityAreaInfo.cityName, cityAreaInfo.area, clickProductId)
    }

    import spark.implicits._
    areaProductIdBasicInfoRDD.toDF("city_id", "city_name", "area", "click_product_id")
  }

  /**
    * 获取城市信息
    * @param spark
    */
  def getCityAreaInfo(spark: SparkSession) = {
    val cityAreaInfoArray = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))

    spark.sparkContext.makeRDD(cityAreaInfoArray).map{
      case (cityId, cityName, area) =>
        (cityId, CityAreaInfo(cityId, cityName, area))
    }
  }

  /**
    * 获取测试id和商品id
    * @param spark
    * @param taskParam
    * @return
    */
  def getCityAndProductInfo(spark: SparkSession, taskParam: JSONObject) = {

    val startDate = ParamUtils.getParam(taskParam, MyConstant.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, MyConstant.PARAM_END_DATE)

    val sql = "select city_id, click_product_id from user_visit_action where date >= '" + startDate +
      "' and date <= '" + endDate + "' and click_product_id != -1"

    import spark.implicits._
    spark.sql(sql).as[CityClickProduct].rdd.map{
      case cityIdAndProductId =>
        (cityIdAndProductId.city_id, cityIdAndProductId.click_product_id)
    }

  }


  /**
    * 统计页面转换率
    * @param spark
    * @param taskUUID
    * @param targetPageSplit
    * @param startPageCount
    * @param realPageSplitCountMap
    */
  def getPageConvertRate(spark: SparkSession,
                         taskUUID: String,
                         targetPageSplit: Array[String],
                         startPageCount: Long,
                         realPageSplitCountMap: collection.Map[String, Long]) = {
    val pageSplitRatioMap = new mutable.HashMap[String, Double]()

    var lastPageCount = startPageCount.toDouble

    for (pageSplit <- targetPageSplit) {
      val currentPageSplitCount = realPageSplitCountMap.get(pageSplit).get.toDouble
      val rate = currentPageSplitCount / lastPageCount
      pageSplitRatioMap.put(pageSplit, rate)
      lastPageCount = currentPageSplitCount
    }

    val convertRateStr = pageSplitRatioMap.map {
      case (pageSplit, rate) =>
        pageSplit + "=" + rate
    }.mkString("|")

    println(convertRateStr)

    val pageSplitConvertRate  = PageSplitConvertRate(taskUUID, convertRateStr)

    val pageSplitConvertRateRDD  = spark.sparkContext.makeRDD(Array(pageSplitConvertRate))

    import spark.implicits._
//
//    val pageSplitConverRateDF = pageSplitConvertRateRDD.toDF()
//    val schema1 = pageSplitConverRateDF.schema.add(StructField("id", LongType))
//    val tempRDD1 = pageSplitConverRateDF.rdd.zipWithIndex()
//    val rowRDD1 = tempRDD1.map(x => {
//      Row.merge(x._1, Row(x._2))
//    })
//
//    val pageSplitDF = spark.createDataFrame(rowRDD1, schema1)
    val pageSplitDF = pageSplitConvertRateRDD.toDF()

    pageSplitDF.write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(MyConstant.JDBC_URL))
      .option("driver", ConfigurationManager.config.getString(MyConstant.JDBC_DRIVER))
      .option("user", ConfigurationManager.config.getString(MyConstant.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(MyConstant.JDBC_PASSWORD))
      .option("dbtable", "page_convert_rate2")
      .mode(SaveMode.Overwrite)
      .option("truncate", "false") //是否删除表进行重建
      .option("batchsize",10000)
      .option("isolationLevel","NONE")
      .save()
  }

  def top10PopularCategories(spark: SparkSession, taskUUID: String, sessionId2ActionFilterRDD: RDD[(String, UserVisitAction)]) ={
    // 第一步：获取所有发生过点击、下单、付款的 categoryId，注意：其中被点击的 categoryId 只有一个，被下单和被付款的 categoryId 有多个，categoryId 之间使用逗号隔开的
    var cid2CidRDD = sessionId2ActionFilterRDD.flatMap {
      case (sessionId, userVisitAction) =>
        val categoryIdBuffer = new ArrayBuffer[(Long, Long)]()

        // 提取出数据填充 ArrayBuffer
        if (userVisitAction.click_category_id != -1) { // 点击行为
          categoryIdBuffer += ((userVisitAction.click_category_id, userVisitAction.click_category_id)) // 只有第一个 key 有用，第二个 value 任何值都可以，但是不可以没有
        } else if (userVisitAction.order_category_ids != null) { // 下单行为
          for (order_category_id <- userVisitAction.order_category_ids.split(",")) {
            categoryIdBuffer += ((order_category_id.toLong, order_category_id.toLong))
          }
        } else if (userVisitAction.pay_category_ids != null) { // 付款行为
          for (pay_category_id <- userVisitAction.pay_category_ids.split(",")) {
            categoryIdBuffer += ((pay_category_id.toLong, pay_category_id.toLong))
          }
        }

        categoryIdBuffer
    }

    // 第二步：进行去重操作
    cid2CidRDD = cid2CidRDD.distinct()

    // 第三步：统计各品类 被点击的次数、被下单的次数、被付款的次数
    val cid2ClickCountRDD = getClickCount(sessionId2ActionFilterRDD)
    val cid2OrderCountRDD = getOrderCount(sessionId2ActionFilterRDD)
    val cid2PayCountRDD = getPayCount(sessionId2ActionFilterRDD)

    // 第四步：获取各个 categoryId 的点击次数、下单次数、付款次数，并进行拼装
    // cid2FullCountRDD: RDD[(cid, aggrCountInfo)]
    // (81,categoryId=81|clickCount=68|orderCount=64|payCount=72)
    val cid2FullCountRDD = getFullCount(cid2CidRDD, cid2ClickCountRDD, cid2OrderCountRDD, cid2PayCountRDD)

    // 第五步：根据点击次数、下单次数、付款次数依次排序，会用到 【二次排序】，实现自定义的二次排序的 key

    // 第六步：封装 SortKey
    val sortKey2FullCountRDD = cid2FullCountRDD.map {
      case (cid, fullCountInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(fullCountInfo, "\\|", MyConstant.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(fullCountInfo, "\\|", MyConstant.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(fullCountInfo, "\\|", MyConstant.FIELD_PAY_COUNT).toLong

        val sortKey = SortKey(clickCount, orderCount, payCount)
        (sortKey, fullCountInfo)
    }

    // 第七步：降序排序，取出 top10 热门品类
    val top10CategoryArray = sortKey2FullCountRDD.sortByKey(false).take(10)

    // 第八步：将 Array 结构转化为 RDD，封装 Top10Category
    val top10CategoryRDD = spark.sparkContext.makeRDD(top10CategoryArray).map {
      case (sortKey, fullCountInfo) =>
        val categoryid = StringUtils.getFieldFromConcatString(fullCountInfo, "\\|", MyConstant.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCount
        val orderCount = sortKey.orderCount
        val payCount = sortKey.payCount

        Top10Category(taskUUID, categoryid, clickCount, orderCount, payCount)
    }

    // 第九步：写入 MySQL 数据库
    import spark.implicits._
    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(MyConstant.JDBC_URL))
      .option("dbtable", "top10_category")
      .option("user", ConfigurationManager.config.getString(MyConstant.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(MyConstant.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

    top10CategoryArray

  }

  /**
    *
    * @param cid2CidRDD
    * @param cid2ClickCountRDD
    * @param cid2OrderCountRDD
    * @param cid2PayCountRDD
    * @return
    */
  def getFullCount(cid2CidRDD: RDD[(Long, Long)],
                   cid2ClickCountRDD: RDD[(Long, Long)],
                   cid2OrderCountRDD: RDD[(Long, Long)],
                   cid2PayCountRDD: RDD[(Long, Long)]) = {
    // 左外连接：不符合添加显示为空（null）

    // 4.1 所有品类id 和 被点击的品类 做左外连接
    val cid2ClickInfoRDD = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map {
      case (cid, (categoryId, option)) =>
        val clickCount = if (option.isDefined) option.get else 0
        val aggrCountInfo = MyConstant.FIELD_CATEGORY_ID + "=" + cid + "|" + MyConstant.FIELD_CLICK_COUNT + "=" + clickCount

        (cid, aggrCountInfo)
    }
    // 4.2 4.1 的结果 和 被下单的品类 做左外连接
    val cid2OrderInfoRDD = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map {
      case (cid, (clickInfo, option)) =>
        val orderCount = if (option.isDefined) option.get else 0
        val aggrCountInfo = clickInfo + "|" + MyConstant.FIELD_ORDER_COUNT + "=" + orderCount

        (cid, aggrCountInfo)
    }
    // 4.3 4.2 的结果 和 被付款的品类 做左外连接
    val cid2PayInfoRDD = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map {
      case (cid, (orderInfo, option)) =>
        val payCount = if (option.isDefined) option.get else 0
        val aggrCountInfo = orderInfo + "|" + MyConstant.FIELD_PAY_COUNT + "=" + payCount

        (cid, aggrCountInfo)
    }

    cid2PayInfoRDD
  }

  /**
    * 统计各品类被点击的次数
    *
    * @param seeionId2ActionFilterRDD
    */
  def getClickCount(seeionId2ActionFilterRDD: RDD[(String, UserVisitAction)]) = {
    // 方式一：把发生过点击的 action 过滤出来
    val clickActionFilterRDD = seeionId2ActionFilterRDD.filter {
      case (sessionId, userVisitAction) =>
        userVisitAction.click_category_id != 1L
    }
    // 方式二：把发生点击的 action 过滤出来，二者等价
    // val clickActionFilterRDD2 = seeionId2ActionFilterRDD.filter(item => item._2.click_category_id != -1L)

    // 获取每种类别的点击次数
    val clickNumRDD = clickActionFilterRDD.map {
      case (sessionId, userVisitAction) =>
        (userVisitAction.click_category_id, 1L)
    }
    // 计算各个品类的点击次数
    clickNumRDD.reduceByKey(_ + _)
  }

  /**
    * 统计各品类被下单的次数
    *
    * @param seeionId2ActionFilterRDD
    */
  def getOrderCount(seeionId2ActionFilterRDD: RDD[(String, UserVisitAction)]) = {
    // 把发生过下单的 action 过滤出来
    val orderActionFilterRDD = seeionId2ActionFilterRDD.filter {
      case (sessionId, userVisitAction) =>
        userVisitAction.order_category_ids != null
    }
    // 获取每种类别的下单次数
    val orderNumRDD = orderActionFilterRDD.flatMap {
      case (sessionId, userVisitAction) =>
        userVisitAction.order_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    // 计算各个品类的下单次数
    orderNumRDD.reduceByKey(_ + _)
  }

  /**
    * 统计各品类被付款的次数
    *
    * @param seeionId2ActionFilterRDD
    */
  def getPayCount(seeionId2ActionFilterRDD: RDD[(String, UserVisitAction)]) = {
    // 把发生过付款的 action 过滤出来
    val payActionFilterRDD = seeionId2ActionFilterRDD.filter {
      case (sessionId, userVisitAction) =>
        userVisitAction.pay_category_ids != null
    }
    // 获取每种类别的支付次数
    val payNumRDD = payActionFilterRDD.flatMap {
      case (sessionId, userVisitAction) =>
        userVisitAction.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    // 计算各个品类的支付次数
    payNumRDD.reduceByKey(_ + _)
  }

  def generateRandomIndexList(extractPerDay: Int,
                              dateCount: Long,
                              hourCountMap: mutable.HashMap[String, Long],
                              hourListMap: mutable.HashMap[String, ListBuffer[Int]]) = {
    for ((hour, count) <- hourCountMap) {
      var hourExtractCount = ((count / dateCount.toDouble) * extractPerDay).toInt

      if (hourExtractCount > count) {
        hourExtractCount = count.toInt
      }

      val random = new Random()

      hourListMap.get(hour) match {
        case None =>
          hourListMap(hour) = new mutable.ListBuffer[Int]()
          for (i <- 0 until hourExtractCount) {
            var index = random.nextInt(count.toInt) // 生成 index
            while (hourListMap(hour).contains(index)) { // 如果 index 已存在
              index = random.nextInt(count.toInt) // 则重新生成 index
            }

            // 将生成的 index 放入到 hourListMap 中
            hourListMap(hour).append(index)
          }
        case Some(map) =>
          for (i <- 0 until hourExtractCount) {
            var index = random.nextInt(count.toInt) // 生成 index
            while (hourListMap(hour).contains(index)) { // 如果 index 已存在
              index = random.nextInt(count.toInt) // 则重新生成 index
            }

            // 将生成的 index 放入到 hourListMap 中
            hourListMap(hour).append(index)
          }
      }
    }
  }

  def sessionRandomExtract(spark: SparkSession, taskUUID: String, sessionId2FilterRDD: RDD[(String, String)]) = {

    val dateHour2FullAggrInfoRDD = sessionId2FilterRDD.map {
      case (sessionId, fullAggrInfo) =>
        val startTime = StringUtils.getFieldFromConcatString(fullAggrInfo, "|", MyConstant.FIELD_START_TIME)
        val dateHour = DateUtils.getDateHour(startTime)

        (dateHour, fullAggrInfo)
    }


    //(yyyy-MM-dd_HH,20)
    val hourCountMap = dateHour2FullAggrInfoRDD.countByKey()

    // dateHourCountMap: Map[date, Map[(hour, count)]]，示例：(yyyy-MM-dd, (HH, 20))
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    for ((dateHour, count) <- hourCountMap) {
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      dateHourCountMap.get(date) match {
        case None =>
          dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourCountMap(date) += (hour -> count)
        case Some(map) =>
          dateHourCountMap(date) += (hour -> count)
      }

    }

    // 解决问题一：
    //   一共有多少天：dateHourCountMap.size
    //   一天抽取多少条：1000 / dateHourCountMap.size
    val extractPerDay = 1000 / dateHourCountMap.size

    // 解决问题二：
    //   一共有多少个session：dateHourCountMap(date).values.sum
    //   一个小时有多少个session：dateHourCountMap(date)(hour)

    val dateHourExtractIndexListMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()

    for ((date, hourCountMap) <- dateHourCountMap) {

      //一共有多少个session
      val dateCount = hourCountMap.values.sum

      dateHourExtractIndexListMap.get(date) match {
        case None =>
          dateHourExtractIndexListMap(date) = new mutable.HashMap[String, ListBuffer[Int]]()
          generateRandomIndexList(extractPerDay, dateCount, hourCountMap, dateHourExtractIndexListMap(date))
        case Some(map) =>
          generateRandomIndexList(extractPerDay, dateCount, hourCountMap, dateHourExtractIndexListMap(date))
      }
    }

    // 到此为止，我们获得了每个小时要抽取的 session 的 index
    // 之后在算子中使用 dateHourExtractIndexListMap 这个 Map，由于这个 Map 可能会很大，所以涉及到 广播大变量 的问题

    // 广播大变量，提升任务 task 的性能
    val dateHourExtractIndexListMapBroadcastVar = spark.sparkContext.broadcast(dateHourExtractIndexListMap)

    val dateHour2GroupRDD = dateHour2FullAggrInfoRDD.groupByKey()

    val extractSessionRDD = dateHour2GroupRDD.flatMap{
      case (dateHour, iterableFullAggrInfo) =>
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)

        val extractIndexList  = dateHourExtractIndexListMapBroadcastVar.value.get(date).get(hour)

        // 创建一个容器存储抽取的 session
        val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()
        var index = 0
        for (fullAggrInfo <- iterableFullAggrInfo) {
          if (extractIndexList.contains(index)) {
            // 提取数据，封装成所需要的样例类，并追加进 ArrayBuffer 中
            val sessionId = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", MyConstant.FIELD_SESSION_ID)
            val startTime = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", MyConstant.FIELD_START_TIME)
            val searchKeywords = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", MyConstant.FIELD_SEARCH_KEYWORDS)
            val clickCategoryIds = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", MyConstant.FIELD_CLICK_CATEGORY_IDS)

            val sessionRandomExtract = SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategoryIds)

            extractSessionArrayBuffer += sessionRandomExtract
          }
          index += 1

        }

        extractSessionArrayBuffer
    }

    import spark.implicits._
    extractSessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(MyConstant.JDBC_URL))
      .option("dbtable", "session_random_extract")
      .option("user", ConfigurationManager.config.getString(MyConstant.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(MyConstant.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }



  def getSessionRatio(spark: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]) = {

    val session_count = value.getOrElse(MyConstant.SESSION_COUNT, 1)

    // 先获取各个值
    val visit_length_1s_3s = value.getOrElse(MyConstant.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(MyConstant.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(MyConstant.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(MyConstant.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(MyConstant.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(MyConstant.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(MyConstant.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(MyConstant.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(MyConstant.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(MyConstant.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(MyConstant.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(MyConstant.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(MyConstant.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(MyConstant.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(MyConstant.STEP_PERIOD_60, 0)

    // 计算比例
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    // 封装数据
    val stat = SessionAggrStat(taskUUID, session_count.toInt,
      visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val sessionRatioRDD = spark.sparkContext.makeRDD(Array(stat))

    val driver = ConfigurationManager.config.getString(MyConstant.JDBC_DRIVER)
    val url = ConfigurationManager.config.getString(MyConstant.JDBC_URL)
    val user = ConfigurationManager.config.getString(MyConstant.JDBC_USER)
    val password = ConfigurationManager.config.getString(MyConstant.JDBC_PASSWORD)

    import spark.implicits._
    sessionRatioRDD.toDF().write
      .format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", password)
      .option("dbtable", "session_aggr_stat")
      .mode(SaveMode.Append)
      .save()
  }


  def calculateVisitLength(visitLength: Long, sessionStatisticAccumulator: SessionStatisticAccumulator) = {
    if (visitLength >= 1 && visitLength <= 3) {
      sessionStatisticAccumulator.add(MyConstant.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionStatisticAccumulator.add(MyConstant.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionStatisticAccumulator.add(MyConstant.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionStatisticAccumulator.add(MyConstant.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionStatisticAccumulator.add(MyConstant.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionStatisticAccumulator.add(MyConstant.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionStatisticAccumulator.add(MyConstant.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionStatisticAccumulator.add(MyConstant.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionStatisticAccumulator.add(MyConstant.TIME_PERIOD_30m)
    }
  }

  def calculateStepLength(stepLength: Long, sessionStatisticAccumulator: SessionStatisticAccumulator) = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionStatisticAccumulator.add(MyConstant.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatisticAccumulator.add(MyConstant.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatisticAccumulator.add(MyConstant.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatisticAccumulator.add(MyConstant.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatisticAccumulator.add(MyConstant.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatisticAccumulator.add(MyConstant.STEP_PERIOD_60)
    }
  }

  def getSessionFilterRDD(taskParam: JSONObject,
                          sessionId2FullAggrInfoRDD: RDD[(String, String)],
                          sessionStatisticAccumulator: SessionStatisticAccumulator) = {
    // 先获取所用到的过滤条件：
    val startAge = ParamUtils.getParam(taskParam, MyConstant.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, MyConstant.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, MyConstant.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, MyConstant.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, MyConstant.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, MyConstant.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, MyConstant.PARAM_CATEGORY_IDS)

    // 拼接过滤条件的字符串：
    var filterInfo = (if (startAge != null) MyConstant.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) MyConstant.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) MyConstant.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) MyConstant.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) MyConstant.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) MyConstant.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) MyConstant.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    // 去除过滤条件字符串末尾的 "|"
    if (filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)

    // 进行过滤操作（过滤自带遍历功能）
    sessionId2FullAggrInfoRDD.filter {
      case (sessionId, fullAggrInfo) =>
        var success = true

        // 如果 age 不在过滤条件范围之内，则当前 sessionId 对应的 fullAggrInfo 数据被过滤掉
        if (!ValidUtils.between(fullAggrInfo, MyConstant.FIELD_AGE, filterInfo, MyConstant.PARAM_START_AGE, MyConstant.PARAM_END_AGE)) { // 范围用 between
          success = false
        } else if (!ValidUtils.in(fullAggrInfo, MyConstant.FIELD_PROFESSIONAL, filterInfo, MyConstant.PARAM_PROFESSIONALS)) {
          success = false
        } else if (!ValidUtils.in(fullAggrInfo, MyConstant.FIELD_CITY, filterInfo, MyConstant.PARAM_CITIES)) {
          success = false
        } else if (!ValidUtils.equal(fullAggrInfo, MyConstant.FIELD_SEX, filterInfo, MyConstant.PARAM_SEX)) { // 二选一用 equal
          success = false
        } else if (!ValidUtils.in(fullAggrInfo, MyConstant.FIELD_SEARCH_KEYWORDS, filterInfo, MyConstant.PARAM_KEYWORDS)) { // 多选一用 in
          success = false
        } else if (!ValidUtils.in(fullAggrInfo, MyConstant.FIELD_CLICK_CATEGORY_IDS, filterInfo, MyConstant.PARAM_CATEGORY_IDS)) {
          success = false
        }

        // 自定义累加器，统计不同范围的 访问时长 和 访问步长 的个数 以及 总的 session 个数
        if (success) {
          sessionStatisticAccumulator.add(MyConstant.SESSION_COUNT) // 总的 session 个数

          // 获取当前 sessionId 对应的 访问时长 和 访问步长
          val visitLength = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", MyConstant.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", MyConstant.FIELD_STEP_LENGTH).toLong

          // 统计不同范围的 访问时长 和 访问步长 的个数
          calculateVisitLength(visitLength, sessionStatisticAccumulator)
          calculateStepLength(stepLength, sessionStatisticAccumulator)
        }

        success
    }
  }

  def getSessionFullAggrInfo(spark: SparkSession, sessionId2ActionGroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {

    val userId2PartAggrInfoRDD = sessionId2ActionGroupRDD.map {
      // 使用模式匹配：当结果是 KV 对的时候尽量使用 case 模式匹配，这样更清楚，更简洁直观
      case (sessionId, iterableAction) =>

        var userId = -1L

        var startTime: Date = null
        var endTime: Date = null

        var stepLength = 0 // 有多少个 action

        val searchKeywords = new StringBuffer("") // 搜索行为
      val clickCategories = new StringBuffer("") // 点击行为

        for (action <- iterableAction) {
          if (userId == -1) {
            userId = action.user_id
          }

          val actionTime = DateUtils.parseTime(action.action_time) // action_time = "2019-05-30 18:17:11" 是字符串类型
          if (startTime == null || startTime.after(actionTime)) { // startTime 在 actionTime 的后面   正常区间：[startTime, actionTime, endTime]
            startTime = actionTime
          }

          if (endTime == null || endTime.before(actionTime)) { // endTime 在 actionTime 的前面
            endTime = actionTime
          }

          //获取搜索行为
          val searchKeyword = action.search_keyword
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)) {
            searchKeywords.append(searchKeyword + ",")
          }

          //获取点击行为
          val clickCategoryId = action.click_category_id
          if (clickCategoryId != -1 && !clickCategories.toString.contains(clickCategoryId)) {
            clickCategories.append(clickCategoryId + ",")
          }

          stepLength += 1
        }

        // searchKeywords.toString.substring(0, searchKeywords.toString.length - 1) // 等价于下面
        val searchKw = StringUtils.trimComma(searchKeywords.toString) // 去除最后一个逗号
        val clickCg = StringUtils.trimComma(clickCategories.toString) // 去除最后一个逗号

        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        // 拼装聚合数据的字符串：
        // (31,sessionid=7291cc307f96432f8da9d926fd7d88e5|searchKeywords=洗面奶,小龙虾,机器学习,苹果,华为手机|clickCategoryIds=11,93,36,66,
        // 60|visitLength=3461|stepLength=43|startTime=2019-05-30 14:01:01)
        val partAggrInfo = MyConstant.FIELD_SESSION_ID + "=" + sessionId + "|" +
          MyConstant.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          MyConstant.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          MyConstant.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          MyConstant.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          MyConstant.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) // 格式化时间为字符串类型

        (userId, partAggrInfo)
    }


    import spark.implicits._

    //将dataframe转换成rdd
    val userInfoRDD = spark.sql("select * from user_info").as[UserInfo].rdd
    
    val userId2InfoRDD = userInfoRDD.map(item => (item.user_id, item))

    //userId2PartAggrInfoRDD: RDD[userId, partAggrInfo]
    //userId2InfoRDD: RDD[userId, userInfo]
    val sessionId2FullAggrInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD).map {
      //按照相同key进行join：(key,(value1, value2))
      case (userId, (partAggrInfo, userInfo)) =>
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        // 拼装最终的聚合数据字符串：
        val fullAggrInfo = partAggrInfo + "|" +
          MyConstant.FIELD_AGE + "=" + age + "|" +
          MyConstant.FIELD_PROFESSIONAL + "=" + professional + "|" +
          MyConstant.FIELD_SEX + "=" + sex + "|" +
          MyConstant.FIELD_CITY + "=" + city

        val seesionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", MyConstant.FIELD_SESSION_ID)

        (seesionId, fullAggrInfo)
    }

    sessionId2FullAggrInfoRDD
    
  }


  def getActionRDD(spark: SparkSession, taskParam: JSONObject) = {

    val startDate = ParamUtils.getParam(taskParam, MyConstant.PARAM_START_DATE)

    val endDate = ParamUtils.getParam(taskParam, MyConstant.PARAM_END_DATE)

//    println(startDate + "----------" + endDate)

    import spark.implicits._

    val sql =
      s"""
        |select * from user_visit_action where date >= "$startDate" and date <= "$endDate"
      """.stripMargin

    val userVisitActionDF = spark.sql(sql)

    userVisitActionDF.as[UserVisitAction].rdd
  }

}
