package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis


// 使用布隆过滤器对uv进行去重
object UvWithBloom {
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
      .map(data => ("uv", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())


  }
}



// 自定义窗口触发器
class MyTrigger() extends Trigger[(String, Long), TimeWindow]{
  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}

  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    // 每来一条数据，就直接触发窗口操作，并清空所有窗口状态
    TriggerResult.FIRE_AND_PURGE
  }
}

// 定义一个布隆过滤器
class Bloom(size:Long) extends Serializable{
  // 位图的总大小
  private val cop = if(size > 0) size else 1 << 27

  // 定义hash函数
  def hash(value:String, seed: Int): Long ={

    var result: Long = 0L
    for( i <- 0 until value.length){
      result = result * seed + value.charAt(i)
    }
    result & (cop - 1)
  }
}


class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]{

  // 定义redis连接
  lazy val jedis = new Jedis()

  lazy val bloom = new Bloom(1 << 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // 位图的存储方式，key是windowEnd，value是bitmap
    val storeKey = context.window.getEnd.toString
    var count = 0L

    // 把每个窗口的uv count值也存入redis表，存放内容为(wimdowEnd -> uvCount)，所以要先从redis中读取
    if( jedis.hget("count", storeKey) != null ){
      count = jedis.hget("count", storeKey).toLong
    }
    // 用布隆过滤器判断当前用户是否已经存在
    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)
    // 定义一个标志位，判断redis位图中有没有这一位
    val isExist = jedis.getbit(storeKey, offset)
    if(!isExist){
      // 如果不存在，位图对应位置1，count + 1
      jedis.setbit(storeKey, offset, true)
      jedis.hset( "count", storeKey, (count + 1).toString )

      out.collect(UvCount(storeKey.toLong, count + 1))
    }else{
      out.collect(UvCount(storeKey.toLong, count))
    }

  }
}

