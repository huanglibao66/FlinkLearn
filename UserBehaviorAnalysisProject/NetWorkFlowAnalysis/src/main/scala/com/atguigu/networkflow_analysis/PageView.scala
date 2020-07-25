package com.atguigu.networkflow_analysis
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


// 定义输入数据的样例类
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)



// 统计 pv
object PageView {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 用相对路径定义数据源
    val resource = getClass.getResource("/UserBehavior.csv")

    val datastream = env.readTextFile(resource.getPath)
      .map(data =>{
        val dataArray: Array[String] = data.split(",")
        UserBehavior( dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps( _.timestamp * 1000L)
      .filter( _.behavior == "pv")  // 只统计pv操作
      .map( data => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    datastream.print("pv count")

    env.execute("page view job")

  }
}
