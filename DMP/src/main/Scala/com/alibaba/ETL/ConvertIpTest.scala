package com.alibaba.ETL

import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

object ConvertIpTest {
  def main(args: Array[String]): Unit = {
    // a. 创建DbSearch对象，传递字典文件
    val dbSearcher = new DbSearcher(new DbConfig(), "D:\\IDEA_data\\DMP\\src\\main\\resources\\dataset\\ip2region.db")
    // b. 依据IP地址解析
    val dataBlock: DataBlock = dbSearcher.btreeSearch("221.222.21.59")
    // 中国|0|海南省|海口市|教育网
    val region: String = dataBlock.getRegion
    println(s"$region")
    // c. 分割字符串，获取省份和城市
    val Array(_, _, province, city, _) = region.split("\\|")
    println(s"省份 = $province, 城市 = $city")
  }
}

