package com.bigdata.commons.model

object DataModel {

}

/**
  * 用户访问动作表
  *
  * @param date               用户点击行为的日期
  * @param user_id            用户的 ID
  * @param session_id         Session 的 ID
  * @param page_id            某个页面的 ID
  * @param action_time        点击行为的时间点
  * @param search_keyword     用户搜索的关键词
  * @param click_category_id  某一个商品品类的 ID
  * @param click_product_id   某一个商品的 ID
  * @param order_category_ids 一次订单中所有品类的 ID 集合
  * @param order_product_ids  一次订单中所有商品的 ID 集合
  * @param pay_category_ids   一次支付中所有品类的 ID 集合
  * @param pay_product_ids    一次支付中所有商品的 ID 集合
  * @param city_id            城市 ID
  */
case class UserVisitAction(date: String,
                           user_id: Long,
                           session_id: String,
                           page_id: Long,
                           action_time: String,
                           search_keyword: String,
                           click_category_id: Long,
                           click_product_id: Long,
                           order_category_ids: String,
                           order_product_ids: String,
                           pay_category_ids: String,
                           pay_product_ids: String,
                           city_id: Long)

/**
  * 用户信息表
  *
  * @param user_id      用户的 ID
  * @param username     用户的名称
  * @param name         用户的名字
  * @param age          用户的年龄
  * @param professional 用户的职业
  * @param city         用户所在的城市
  * @param sex          用户的性别
  */
case class UserInfo(user_id: Long,
                    username: String,
                    name: String,
                    age: Int,
                    professional: String,
                    city: String,
                    sex: String)

/**
  * 产品表
  *
  * @param product_id   商品的 ID
  * @param product_name 商品的名称
  * @param extend_info  商品额外的信息
  */
case class ProductInfo(product_id: Long,
                       product_name: String,
                       extend_info: String)

/**
  * Session 聚合统计表
  *
  * @param taskid                     当前计算批次的 ID
  * @param session_count              所有 Session 的总和
  * @param visit_length_1s_3s_ratio   1-3s Session 访问时长占比
  * @param visit_length_4s_6s_ratio   4-6s Session 访问时长占比
  * @param visit_length_7s_9s_ratio   7-9s Session 访问时长占比
  * @param visit_length_10s_30s_ratio 10-30s Session 访问时长占比
  * @param visit_length_30s_60s_ratio 30-60s Session 访问时长占比
  * @param visit_length_1m_3m_ratio   1-3m Session 访问时长占比
  * @param visit_length_3m_10m_ratio  3-10m Session 访问时长占比
  * @param visit_length_10m_30m_ratio 10-30m Session 访问时长占比
  * @param visit_length_30m_ratio     30m Session 访问时长占比
  * @param step_length_1_3_ratio      1-3 步长占比
  * @param step_length_4_6_ratio      4-6 步长占比
  * @param step_length_7_9_ratio      7-9 步长占比
  * @param step_length_10_30_ratio    10-30 步长占比
  * @param step_length_30_60_ratio    30-60 步长占比
  * @param step_length_60_ratio       大于 60 步长占比
  */
case class SessionAggrStat(taskid: String,
                           session_count: Long,
                           visit_length_1s_3s_ratio: Double,
                           visit_length_4s_6s_ratio: Double,
                           visit_length_7s_9s_ratio: Double,
                           visit_length_10s_30s_ratio: Double,
                           visit_length_30s_60s_ratio: Double,
                           visit_length_1m_3m_ratio: Double,
                           visit_length_3m_10m_ratio: Double,
                           visit_length_10m_30m_ratio: Double,
                           visit_length_30m_ratio: Double,
                           step_length_1_3_ratio: Double,
                           step_length_4_6_ratio: Double,
                           step_length_7_9_ratio: Double,
                           step_length_10_30_ratio: Double,
                           step_length_30_60_ratio: Double,
                           step_length_60_ratio: Double)

/**
  * Session 随机抽取表
  *
  * @param taskid           当前计算批次的 ID
  * @param sessionid        抽取的 Session 的 ID
  * @param startTime        Session 的开始时间
  * @param searchKeywords   Session 的查询字段
  * @param clickCategoryIds Session 点击的类别 id 集合
  */
case class SessionRandomExtract(taskid: String,
                                sessionid: String,
                                startTime: String,
                                searchKeywords: String,
                                clickCategoryIds: String)

/**
  * Session 随机抽取详细表
  *
  * @param taskid           当前计算批次的 ID
  * @param userid           用户的 ID
  * @param sessionid        Session的 ID
  * @param pageid           某个页面的 ID
  * @param actionTime       点击行为的时间点
  * @param searchKeyword    用户搜索的关键词
  * @param clickCategoryId  某一个商品品类的 ID
  * @param clickProductId   某一个商品的 ID
  * @param orderCategoryIds 一次订单中所有品类的 ID 集合
  * @param orderProductIds  一次订单中所有商品的 ID 集合
  * @param payCategoryIds   一次支付中所有品类的 ID 集合
  * @param payProductIds    一次支付中所有商品的 ID 集合
  **/
case class SessionDetail(taskid: String,
                         userid: Long,
                         sessionid: String,
                         pageid: Long,
                         actionTime: String,
                         searchKeyword: String,
                         clickCategoryId: Long,
                         clickProductId: Long,
                         orderCategoryIds: String,
                         orderProductIds: String,
                         payCategoryIds: String,
                         payProductIds: String)

/**
  * 品类 Top10 表
  *
  * @param taskid
  * @param categoryid
  * @param clickCount
  * @param orderCount
  * @param payCount
  */
case class Top10Category(taskid: String,
                         categoryid: Long,
                         clickCount: Long,
                         orderCount: Long,
                         payCount: Long)

/**
  * Top10 Session
  *
  * @param taskid
  * @param categoryid
  * @param sessionid
  * @param clickCount
  */
case class Top10Session(taskid: String,
                        categoryid: Long,
                        sessionid: String,
                        clickCount: Long)


/**
  *
  * @param taskid
  * @param convertRate
  */
case class PageSplitConvertRate(taskid: String,
                                convertRate: String
                               )


case class CityClickProduct(city_id: Long,
                            click_product_id: Long)


case class CityAreaInfo(cityId: Long,
                        cityName: String,
                        area: String)

/**
  * 广告黑名单
  *
  * @author
  *
  */
case class AdBlacklist(userid: Long)

/**
  * 用户广告点击量
  *
  * @author
  *
  */
case class AdUserClickCount(date: String,
                            userid: Long,
                            adid: Long,
                            clickCount: Long)