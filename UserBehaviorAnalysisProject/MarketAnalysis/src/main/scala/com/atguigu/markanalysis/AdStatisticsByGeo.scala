package com.atguigu.markanalysis

import java.net.URL

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


// 输入的广告点击事件样例类
case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

// 按照省份统计的输出结果样例类
case class CountByProvince(windowEnd: Long, province: String, count: Long)

// 输出的黑名单报警信息
case class BlackListWarning(userId: Long, adId: Long, msg: String)


object AdStatisticsByGeo {

  // 定义测输出流的tag
  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")


  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource: URL = getClass.getResource("AdClickLog.csv")

    // 读取数据并转换成AdClickEvent
    val adEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)


    // 自定义process function，过滤大量刷单击行为
    val filterBlackListStream = adEventStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(100))


    // 根据省份做分组，开窗聚合
    val adCountStream = filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())

    adCountStream.print("count")
    filterBlackListStream.getSideOutput(blackListOutputTag).print("blackList")
    env.execute("ad statistics job")

  }


  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {

    // 定义状态，保存当前用户对当前广告的点击量
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("value-state", classOf[Long]))

    // 保存是否发送过黑名单的状态
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issend-state", classOf[Boolean]))

    // 保存定时器出发的时间戳
    lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))

    override def processElement(value: AdClickEvent, context: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, collector: Collector[AdClickEvent]): Unit = {
      // 取出count状态
      val currentCount = countState.value()

      // 如果是第一次处理，注册定时器，每天00:00触发
      if (currentCount == 0) {
        val ts = (context.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) / (1000 * 60 * 60 * 24)
        resetTimer.update(ts)
        context.timerService().registerProcessingTimeTimer(ts)
      }

      // 判断计数是否达到上限，如果达到则加入黑名单
      if( currentCount >= maxCount){
        // 判断是否发送过黑名单，只发送一次
        if( !isSentBlackList.value() ){
          isSentBlackList.update(true)
          // 输出到侧输出流
          context.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, "click over " + maxCount + " times today."))
        }
        return
      }

      //计数状态加1，输出数据到主流
      countState.update(currentCount + 1)
      collector.collect(value)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      // 定时器触发时，清空状态
      if(timestamp == resetTimer.value()){
        isSentBlackList.clear()
        resetTimer.clear()
        countState.clear()
      }

    }
  }
}


// 自定义预聚合函数
class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
  override def add(in: AdClickEvent, acc: Long): Long = acc + 1

  override def createAccumulator(): Long = 0L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}


// 自定义窗口处理函数
class AdCountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {

    out.collect(CountByProvince(window.getEnd, key, input.iterator.next()))

  }
}
