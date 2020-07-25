package com.atguigu.orderpay_detect

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object OrderTimeoutTagWithoutCep {

  val orderTimeOutputTag = new OutputTag[OrderEvent]("order time out")

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
    //    val timeoutWarningStream = orderEventStream.process(new OrderTimeoutWarning())

    val orderResultStream = orderEventStream.process(new OrderPayMatch())


    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeOutputTag).print("timeout")
    env.execute("order timeout cep.job")
  }


  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

    // 保存定时器的时间戳为状态
    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state", classOf[Long]))

    override def processElement(value: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {

      // 先读取状态
      val isPayed = isPayedState.value()
      val timerTs = timerState.value()

      // 根据事件的类型进行分类判断，做不同的逻辑处理
      if (value.eventType == "create") {
        // 1.如果是create事件，接下来判断pay是否来过
        if (isPayed) {
          // 1.1如果已经pay过，匹配成功，输出主流，清空状态
          collector.collect(OrderResult(value.orderId, "payed successfully"))
          context.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()
        } else {
          // 1.2如果没有pay过，注册定时器等待pay的到来
          val ts = value.eventTime * 1000L + 15 * 60 * 1000L
          context.timerService().registerEventTimeTimer(ts)
          timerState.update(ts)
        }
      } else if (value.eventType == "pay") {
        // 2.如果是pay事件，那么判断是否create过，用timer表示
        if(timerTs > 0){
          // 2.1如果有定时器，说明已经有create来过

          // 继续判断，是否超过了timeout时间
          if(timerTs > value.eventTime * 1000L){
            // 2.1.1如果定时器事件还没到，那么输出成功匹配
            collector.collect(OrderResult(value.orderId, "payed successfully"))
          }else{
            // 2.1.2如果pay的事件已经超过，那么输出到侧输出流
            context.output(orderTimeOutputTag, OrderResult(value.orderId, "payed but already timeout"))
          }

          //输出结束，清空状态
          context.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()
        }else{
          // 2.2 pay先到了，更新状态，注册定时器等待create
          isPayedState.update(true)

              //注册当前事件时间的定时器，等待create到watermark到达的时间，如果还没到，就说明create已经丢失了
          context.timerService().registerEventTimeTimer(value.eventTime * 1000L)
        }
      }

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      // 根据状态的值，判断哪个数据没来
      if(isPayedState.value()){
        // 如果为true，表示pay先到了，没等到create
        ctx.output(orderTimeOutputTag, OrderResult(ctx.getCurrentKey, "already payed but not found create log"))
      }else{

        // 表示create到了，没有等到pay
        ctx.output(orderTimeOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
      }
      isPayedState.clear()
      timerState.clear()
    }
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
    } else if (value.eventType == "pay") {
      // 如果时pay事件，直接把状态改为true
      isPayedState.update(true)
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 直接判断isPayed是否为true
    val isPayed = isPayedState.value()
    if (!isPayed) {
      out.collect(OrderResult(ctx.getCurrentKey, "order payed successfully"))
    } else {
      out.collect(OrderResult(ctx.getCurrentKey, "order timeout"))
    }

    // 清空状态
    isPayedState.clear()
  }

}

