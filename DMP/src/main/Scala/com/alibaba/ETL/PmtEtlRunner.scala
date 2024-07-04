package com.alibaba.ETL

import com.alibaba.Config.ConfigApplication
import com.alibaba.Utils.{IPUtils, SparkUtils}
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}
import com.alibaba.Utils.Region
/**
 * 广告数据进行ETL处理，具体步骤如下：
 * 第一步、加载json数据
 * 第二步、解析IP地址为省份和城市
 * 第三步、数据保存至Hive表
 * TODO: 基于SparkSQL中DataFrame数据结构，使用DSL编程方式
 */
object PmtEtlRunner {
  /**
   * 对数据进行ETL处理，调用ip2Region第三方库，解析IP地址为省份和城市
   */
  def processData(dataframe: DataFrame): DataFrame = {
    // 获取SparkSession对象，并导入隐式转换
    val spark: SparkSession = dataframe.sparkSession
    import spark.implicits._
    // 解析IP地址数据字典文件分发
    spark.sparkContext.addFile(ConfigApplication.IPS_DATA_REGION_PATH)
    // TODO: 由于DataFrame弱类型（无泛型），不能直接使用mapPartitions或map，建议转换为RDD操作
    // a. 解析IP地址
    val newRowsRDD: RDD[Row] = dataframe.rdd.mapPartitions { iter =>
      // 创建DbSearcher对象，针对每个分区创建一个，并不是每条数据创建一个
      val dbSearcher = new DbSearcher(new DbConfig(), SparkFiles.get("ip2region.db"))
      // 针对每个分区数据操作, 获取IP值，解析为省份和城市
      iter.map { row =>
        // i. 获取IP值
        val ipValue: String = row.getAs[String]("ip")
        // ii. 调用工具类解析ip地址
        val region: Region = IPUtils.convertIpToRegion(ipValue, dbSearcher)
        // iii. 将解析省份和城市追加到原来Row中
        val newSeq: Seq[Any] = row.toSeq :+
          region.province :+
          region.city
        // iv. 返回Row对象
        Row.fromSeq(newSeq)
      }
    }
    // b. 自定义Schema信息
    val newSchema: StructType = dataframe.schema // 获取原来DataFrame中Schema信息
      // 添加新字段Schema信息
      .add("province", StringType, nullable = true)
      .add("city", StringType, nullable = true)
    // c. 将RDD转换为DataFrame
    val df: DataFrame = spark.createDataFrame(newRowsRDD, newSchema)
    // d. 添加一列日期字段，作为分区列
    df.withColumn("date_str", date_sub(current_date(), 1).cast(StringType))
  }
  /**
   * 保存数据至Parquet文件，列式存储
   */
  def saveAsParquet(dataframe: DataFrame): Unit = {
    dataframe
      // 降低分区数目，保存文件时为一个文件
      .coalesce(1)
      .write
      // 选择覆盖保存模式，如果失败再次运行保存，不存在重复数据
      .mode(SaveMode.Overwrite)
      // 设置分区列名称
      .partitionBy("date_str")
      .parquet("dataset/pmt-etl/")
  }
  /**
   * 保存数据至Hive分区表中，按照日期字段分区
   */
  def saveAsHiveTable(dataframe: DataFrame): Unit = {
    dataframe
      .coalesce(1)
      .write
      .format("hive") // 一定要指定为hive数据源，否则报错
      .mode(SaveMode.Append)
      .partitionBy("date_str")
      .saveAsTable("itcast_ads.pmt_ads_info")
  }
  def main(args: Array[String]): Unit = {
    // 设置Spark应用程序运行的用户：root, 默认情况下为当前系统用户
    System.setProperty("user.name", "root")
    System.setProperty("HADOOP_USER_NAME", "root")
    // 1. 创建SparkSession实例对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._
    // 2. 加载json数据
    val pmtDF: DataFrame = spark.read.json(ConfigApplication.DATAS_PATH)
    //pmtDF.printSchema()
    //pmtDF.show(10, truncate = false)
    // 3. 解析IP地址为省份和城市
    val etlDF: DataFrame = processData(pmtDF)
    //etlDF.printSchema()
    //etlDF.select($"ip", $"province", $"city", $"date_str").show(10, truncate = false)
    // 4. 保存ETL数据至Hive分区表
    //saveAsParquet(etlDF)
    saveAsHiveTable(etlDF)
    // 5. 应用结束，关闭资源
    //Thread.sleep(1000000)
    spark.stop()
  }
}

