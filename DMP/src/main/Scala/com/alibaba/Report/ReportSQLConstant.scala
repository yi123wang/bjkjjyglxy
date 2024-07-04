package com.alibaba.Report

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 统计报表的SQL语句
 */
object ReportSQLConstant {
  /**
   * 广告投放的地域分布的SQL语句
   * @param tempViewName DataFrame注册的临时视图名称
   */
  def reportAdsRegionSQL(tempViewName: String): String = {
    // 在Scala语言中，字符串可以使用双引号和三引号
    s"""
       SELECT
       | cast(date_sub(current_date(), 1) AS string) AS report_date,
       | t.province, t.city,
       | SUM(
       | CASE
       | WHEN (t.requestmode = 1 and t. processnode >= 1)
       | THEN 1
       | ELSE 0
       | END
       | ) AS orginal_req_cnt,
       | SUM(
       | CASE
       | WHEN (t.requestmode = 1 and t.processnode >= 2)
       | THEN 1
       | ELSE 0
       | END
       | ) AS valid_req_cnt,
       | SUM(
       | CASE
       | WHEN (t.requestmode = 1 and t.processnode = 3)
       | THEN 1
       | ELSE 0
       | END
       | ) AS ad_req_cnt,
       | SUM(CASE
       | WHEN (t.adplatformproviderid >= 100000
       | AND t.iseffective = 1
       | AND t.isbilling = 1
       | AND t.isbid = 1
       | AND t.adorderid != 0) THEN 1
       | ELSE 0
       | END) AS join_rtx_cnt,
       | SUM(CASE
       | WHEN (t.adplatformproviderid >= 100000
       | AND t.iseffective = 1
       | AND t.isbilling = 1
       | AND t.iswin = 1) THEN 1
       | ELSE 0
       | END) AS success_rtx_cnt,
       | SUM(CASE
       | WHEN (t.requestmode = 2
       | AND t.iseffective = 1) THEN 1
       | ELSE 0
       | END) AS ad_show_cnt,
       | SUM(CASE
       | WHEN (t.requestmode = 3
       | AND t.iseffective = 1) THEN 1
       | ELSE 0
       | END) AS ad_click_cnt,
       | SUM(CASE
       | WHEN (t.requestmode = 2
       | AND t.iseffective = 1
       | AND t.isbilling = 1) THEN 1
       | ELSE 0
       | END) AS media_show_cnt,
       | SUM(CASE
       | WHEN (t.requestmode = 3
       | AND t.iseffective = 1
       | AND t.isbilling = 1) THEN 1
       | ELSE 0
       | END) AS media_click_cnt,
       | SUM(CASE
       | WHEN (t.adplatformproviderid >= 100000
       | AND t.iseffective = 1
       | AND t.isbilling = 1
       | AND t.iswin = 1
       | AND t.adorderid > 200000
       | AND t.adcreativeid > 200000) THEN floor(t.winprice / 1000)
       | ELSE 0
       | END) AS dsp_pay_money,
       | SUM(CASE
       | WHEN (t.adplatformproviderid >= 100000
       | AND t.iseffective = 1
       | AND t.isbilling = 1
       | AND t.iswin = 1
       | AND t.adorderid > 200000
       | AND t.adcreativeid > 200000) THEN floor(t.adpayment / 1000)
       | ELSE 0
       | END) AS dsp_cost_money
       |FROM
       | tmp_view_pmt t
       |GROUP BY
       | t.province, t.city
       |""".stripMargin
  }
  /**
   * 统计竞价成功率、广告点击率、媒体点击率的SQL
   */
  def reportAdsRegionRateSQL(tempViewName: String): String = {
    s"""
       |SELECT
       | t.*,
       | round(t.success_rtx_cnt / t.join_rtx_cnt, 2) AS success_rtx_rate,
       | round(t.ad_click_cnt / t.ad_show_cnt, 2) AS ad_click_rate,
       | round(t.media_click_cnt / t.media_show_cnt, 2) AS media_click_rate
       |FROM
       | $tempViewName t
       |WHERE
       | t.join_rtx_cnt != 0 AND t.success_rtx_cnt != 0
       | AND t.ad_show_cnt != 0 AND t.ad_click_cnt != 0
       | AND t.media_show_cnt != 0 AND t.media_click_cnt != 0
       |""".stripMargin
  }
  /**
   * 使用WITH AS 子查询语句分析
   * @param tempViewName 视图名称
   * @return
   */
  def reportAdsRegionKpiSQL(tempViewName: String): String = {
    s"""
       |WITH tmp AS (
       | SELECT
       | cast(date_sub(current_date(), 1) AS string) AS report_date,
       | t.province, t.city,
       | SUM(
       | CASE
       | WHEN (t.requestmode = 1 and t. processnode >= 1)
       | THEN 1
       | ELSE 0
       | END
       | ) AS orginal_req_cnt,
       | SUM(
       | CASE
       | WHEN (t.requestmode = 1 and t.processnode >= 2)
       | THEN 1
       | ELSE 0
       | END
       | ) AS valid_req_cnt,
       | SUM(
       | CASE
       | WHEN (t.requestmode = 1 and t.processnode = 3)
       | THEN 1
       | ELSE 0
       | END
       | ) AS ad_req_cnt,
       | SUM(CASE
       | WHEN (t.adplatformproviderid >= 100000
       | AND t.iseffective = 1
       | AND t.isbilling = 1
       | AND t.isbid = 1
       | AND t.adorderid != 0) THEN 1
       | ELSE 0
       | END) AS join_rtx_cnt,
       | SUM(CASE
       | WHEN (t.adplatformproviderid >= 100000
       | AND t.iseffective = 1
       | AND t.isbilling = 1
       | AND t.iswin = 1) THEN 1
       | ELSE 0
       | END) AS success_rtx_cnt,
       | SUM(CASE
       | WHEN (t.requestmode = 2
       | AND t.iseffective = 1) THEN 1
       | ELSE 0
       | END) AS ad_show_cnt,
       | SUM(CASE
       | WHEN (t.requestmode = 3
       | AND t.iseffective = 1) THEN 1
       | ELSE 0
       | END) AS ad_click_cnt,
       | SUM(CASE
       | WHEN (t.requestmode = 2
       | AND t.iseffective = 1
       | AND t.isbilling = 1) THEN 1
       | ELSE 0
       | END) AS media_show_cnt,
       | SUM(CASE
       | WHEN (t.requestmode = 3
       | AND t.iseffective = 1
       | AND t.isbilling = 1) THEN 1
       | ELSE 0
       | END) AS media_click_cnt,
       | SUM(CASE
       | WHEN (t.adplatformproviderid >= 100000
       | AND t.iseffective = 1
       | AND t.isbilling = 1
       | AND t.iswin = 1
       | AND t.adorderid > 200000
       | AND t.adcreativeid > 200000) THEN floor(t.winprice / 1000)
       | ELSE 0
       | END) AS dsp_pay_money,
       | SUM(CASE
       | WHEN (t.adplatformproviderid >= 100000
       | AND t.iseffective = 1
       | AND t.isbilling = 1
       | AND t.iswin = 1
       | AND t.adorderid > 200000
       | AND t.adcreativeid > 200000) THEN floor(t.adpayment / 1000)
       | ELSE 0
       | END) AS dsp_cost_money
       | FROM
       | $tempViewName t
       | GROUP BY
       | t.province, t.city
       |)
       |SELECT
       | tt.*,
       | round(tt.success_rtx_cnt / tt.join_rtx_cnt, 2) AS success_rtx_rate,
       | round(tt.ad_click_cnt / tt.ad_show_cnt, 2) AS ad_click_rate,
       | round(tt.media_click_cnt / tt.media_show_cnt, 2) AS media_click_rate
       |FROM
       | tmp tt
       |WHERE
       | tt.join_rtx_cnt != 0 AND tt.success_rtx_cnt != 0
       | AND tt.ad_show_cnt != 0 AND tt.ad_click_cnt != 0
       | AND tt.media_show_cnt != 0 AND tt.media_click_cnt != 0
       |""".stripMargin
  }
  /**
   * 使用SQL方式计算广告投放报表
   */
  def reportWithKpiSql(dataframe: DataFrame): DataFrame = {
    // 从DataFrame中获取SparkSession对象
    val spark: SparkSession = dataframe.sparkSession
    /*
    在SparkSQL中使用SQL分析数据时，步骤分为两步：
    - 第一步、将DataFrame注册为临时视图
    - 第二步、编写SQL语句，使用SparkSession执行
    */
    // i. 注册广告数据集为临时视图：tmp_view_pmt
    dataframe.createOrReplaceTempView("tmp_view_pmt")
    // ii. 编写SQL并执行获取结果
    val kpiSql: String = ReportSQLConstant.reportAdsRegionKpiSQL("tmp_view_pmt")
    //println(kpiSql)
    val reportDF: DataFrame = spark.sql(kpiSql)
    //reportDF.show(20, truncate = false)
    // iii. 返回结果
    reportDF
  }

      /**
   * 使用SQL方式计算广告投放报表
   */
  def reportWithSql(dataframe: DataFrame): DataFrame = {
    // 从DataFrame中获取SparkSession对象
    val spark: SparkSession = dataframe.sparkSession
    /*
    在SparkSQL中使用SQL分析数据时，步骤分为两步：
    - 第一步、将DataFrame注册为临时视图
    - 第二步、编写SQL语句，使用SparkSession执行
    */
    // i. 注册广告数据集为临时视图：tmp_view_pmt
    dataframe.createOrReplaceTempView("tmp_view_pmt")
    // ii. 编写SQL并执行获取结果
    val reportDF: DataFrame = spark.sql(
      ReportSQLConstant.reportAdsRegionSQL("tmp_view_pmt")
    )
    //reportDF.printSchema()
    //reportDF.show(20, truncate = false)
    // iii. 为了计算“三率”首先注册DataFrame为临时视图
    reportDF.createOrReplaceTempView("tmp_view_report")
    // iv. 编写SQL并执行获取结果
    val resultDF: DataFrame = spark.sql(
      ReportSQLConstant.reportAdsRegionRateSQL("tmp_view_report")
    )
//    resultDF.printSchema()
//    resultDF.show(20, truncate = false)
    // iii. 返回结果
    resultDF
  }

}
