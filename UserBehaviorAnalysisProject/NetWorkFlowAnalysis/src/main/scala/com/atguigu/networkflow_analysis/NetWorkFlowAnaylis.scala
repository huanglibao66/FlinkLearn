package com.atguigu.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/**
  * 每隔5秒，输出最近10分钟内访问量最多的前N个URL
  */

// 输出数据样例类
case class ApacheLogEvent( ip: String, userId: String, eventTime: Long, method: String, url: String )

// 窗口聚合结果样例类
case class UrlViewCount(url: String, windowEnd:Long, count: Long)

object NetWorkFlowAnaylis {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[String] = env.readTextFile("")
      .map(data => {
        val dataArray: Array[String] = data.split(" ")
        // 定义时间转换
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp: Long = simpleDateFormat.parse(dataArray(3).trim).getTime
        ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
      override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
    })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.seconds(60))
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))

    dataStream.print()

    env.execute("network flow job")
  }
}


// 自定义聚合函数
class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long]{
  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def createAccumulator(): Long = 0L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}


// 自定义窗口函数
class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}


class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{

  // 保存中间状态
  lazy val urlState:ListState[UrlViewCount] = getRuntimeContext.getListState( new ListStateDescriptor("url-state", classOf[UrlViewCount]))

  override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {

    urlState.add(i)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)

  }

  // 重写触发器，在规定延迟后触发
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    val allUrlViews:ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]

    // 遍历数据
    val iter = urlState.get().iterator()
    while (iter.hasNext){
      allUrlViews += iter.next()
    }

    urlState.clear()
    val sortedUrlView: ListBuffer[UrlViewCount] = allUrlViews.sortWith(_.count > _.count).take(topSize)

    val result = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

    for( i <- sortedUrlView.indices){
      val currentUrlView = sortedUrlView(i)
      result.append("NO").append(i + 1).append(":")
        .append(" URL=").append(currentUrlView.url)
        .append(" 访问量=").append(currentUrlView.count).append("\n")
    }

    result.append("===========================")
    Thread.sleep(1000)
    out.collect(result.toString())

  }
}
