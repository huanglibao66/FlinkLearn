package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

case class UvCount(windowEnd:Long, uvCount: Long)


// 计算
object UniqueVisitor {
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
      .timeWindowAll( Time.hours(1))
      .apply( new UvCountMyWindow())

    datastream.print()
    env.execute("uv job")
  }
}



class UvCountMyWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {

    // 定义一个scala set，用于保存所有的数据userId并去重
    // 数据量如果再大，可以使用redis去重，也可以使用布隆过滤器
    var idSet = Set[Long]()

    // 把当前窗口所有数据的ID收集到set中，最后输出set大小
    for(userBehavior <- input){
      idSet += userBehavior.userId
    }
    out.collect(UvCount( window.getEnd, idSet.size))
  }
}
