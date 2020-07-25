package com.atguigu.orderpay_detect

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object OrderTimeoutWithoutCep {


  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1.读取订单数据
    val resource: URL = getClass.getResource("/OrderLog.csv")
    val orderEventStream: KeyedStream[OrderEvent, Long] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)


    // 定义process function定义超时检测
    val timeoutWarningStream = orderEventStream.process(new OrderTimeoutWarning())

    timeoutWarningStream.print()
    env.execute("order timeout cep.job")
  }
}


// 实现自定义的处理函数
class OrderTimeoutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

  // 保存pay是否来过的状态
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

  override def processElement(value: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {

    //先取出状态标识位
    val isPayed = isPayedState.value()

    if (value.eventType == "create" && !isPayed) {
      // 如果遇到了create事件，并且pay没有来过，注册定时器开始等待
      context.timerService().registerEventTimeTimer(value.eventTime * 1000L + 15 * 60 * 1000L)
    }else if(value.eventType == "pay"){
      // 如果时pay事件，直接把状态改为true
      isPayedState.update(true)
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 直接判断isPayed是否为true
    val isPayed = isPayedState.value()
    if(!isPayed){
      out.collect(OrderResult(ctx.getCurrentKey, "order payed successfully"))
    }else{
      out.collect(OrderResult(ctx.getCurrentKey, "order timeout"))
    }

    // 清空状态
    isPayedState.clear()
  }


}

