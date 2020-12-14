package com.atguigu.spkStr

import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory

object JdbcUtil {
  private  val dataSource=int()
//TODO test
  private def int()={
    val properties = new Properties()
    properties.put("driverClassName", "com.mysql.jdbc.Driver")
    properties.put("url", "jdbc:mysql://hadoop102:3306/atguigu1210")
    properties.setProperty("username", "root")
    properties.setProperty("password", "123456")
    properties.setProperty("maxActive", "20")
    DruidDataSourceFactory.createDataSource(properties)
  }

  def getConnect()={
    dataSource.getConnection()
  }

}
