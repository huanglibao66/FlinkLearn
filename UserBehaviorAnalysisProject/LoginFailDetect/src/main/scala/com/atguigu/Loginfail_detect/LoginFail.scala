package com.atguigu.Loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 使用状态编程来完成业务需求
  */

// 输入的登陆事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

// 输出的异常报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warnMsg: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map(data =>{
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(t: LoginEvent): Long = t.eventTime * 1000L
      })

    val warningStream = loginEventStream
      .keyBy(_.userId) //以用户id做分组
      .process(new LoginWarning(2))

    warningStream.print()
    env.execute("login fail detect job")
  }
}


class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent,Warning]{

  // 定义状态保存两秒内所有登陆失败事件
  lazy val loginFailState:ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, collector: Collector[Warning]): Unit = {
    val loginFaillist = loginFailState.get()

    // 判断类型是否为fail，只添加fail的事件到状态
    if(value.eventType == "fail"){

      if(! loginFaillist.iterator().hasNext){
        context.timerService().registerEventTimeTimer(value.eventTime*1000L + 2000L)
      }
      loginFailState.add(value)
    }else{
      // 如果时成功，清空状态
      loginFailState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    // 触发定时器的时候，根据状态里的失败个数决定是否输出报警

    val allLoginFails:ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
    val iter = loginFailState.get().iterator()
    while (iter.hasNext){
      allLoginFails += iter.next()
    }

    if(allLoginFails.length >= maxFailTimes){
      out.collect(Warning(ctx.getCurrentKey, allLoginFails.head.eventTime, allLoginFails.last.eventTime, "login fail 2 seconds for " + allLoginFails.length + " times."))
    }
    loginFailState.clear()
  }

}



