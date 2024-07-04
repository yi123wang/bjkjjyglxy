package com.alibaba.Utils

import com.alibaba.Config.ConfigApplication
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {

  def createSparkSession(clazz: Class[_]): SparkSession = {
    // 1. 构建SparkConf对象
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(clazz.getSimpleName.stripSuffix("$"))
      // 设置输出文件算法
      .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .set("spark.debug.maxToStringFields", "20000")
    // 2. 判断应用是否本地模式运行，如果是设置值
    if(ConfigApplication.APP_LOCAL_MODE){
      sparkConf
        .setMaster(ConfigApplication.APP_SPARK_MASTER)
        // 设置Shuffle时分区数目
        .set("spark.sql.shuffle.partitions", "4")
    }
    // 3. 创建SparkSession.Builder对象
    var builder: SparkSession.Builder = SparkSession.builder()
      .config(sparkConf)
    // 4. 判断应用是否集成Hive，如果集成，设置HiveMetaStore地址
    if(ConfigApplication.APP_IS_HIVE){
      builder = builder
        .enableHiveSupport()
        .config("hive.metastore.uris", ConfigApplication.APP_HIVE_META_STORE_URLS)
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
    }
    // 5. 获取SparkSession实例对象
    val session: SparkSession = builder.getOrCreate()
    // 6. 返回实例
    session
  }
}