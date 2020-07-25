package com.atguigu.orderpay_detect

import java.net.URL
import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

// 定义输入订单事件的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

// 定义输出结果样例类
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeOut {
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


    // 2.定义一个匹配模式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 3.把模式应用到stream上，得到一个pattern stream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    // 4.调用select方法，提取事件序列，超时的事件要做报警提示
    val orderTimeoutOututTag = new OutputTag[OrderResult]("orderTimeout")

    val resultStream = patternStream.select( orderTimeoutOututTag,
      new OrderTimeoutSelect(),
      new orderPaySelect())

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOututTag).print("timeout")
    env.execute("order timeout job")
  }

}

// 自定义超时事件序列处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId = map.get("begin").iterator().next().orderId //不能get("follow")，因为超时事件中肯定有begin而没有follow
    OrderResult(timeoutOrderId, "timeout")
  }
}


// 自定义正常支付序列处理函数
class orderPaySelect() extends PatternSelectFunction[OrderEvent,OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = map.get("follow").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully")
  }
}
