package com.alibaba.Utils


import com.alibaba.Utils.Region
import org.lionsoul.ip2region.{DataBlock, DbSearcher}
/**
 * IP地址解析工具类
 */

object IPUtils {
  /**
   * IP地址解析为省份和城市
   * @param ip ip地址
   * @param dbSearcher DbSearcher对象
   * @return Region 省份城市信息
   */
  def convertIpToRegion(ip: String, dbSearcher: DbSearcher): Region = {
    // a. 依据IP地址解析
    val dataBlock: DataBlock = dbSearcher.btreeSearch(ip)
    val region: String = dataBlock.getRegion // 中国|0|海南省|海口市|教育网
    // b. 分割字符串，获取省份和城市
    val Array(_, _, province, city, _) = region.split("\\|")
    // c. 返回Region对象
    Region(ip, province, city)
  }
}

