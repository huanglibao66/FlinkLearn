package com.atguigu

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object SendToKafka {
  def main(args: Array[String]): Unit = {
    sendKafka("")
  }

  def sendKafka(topic: String): Unit ={
    val  properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serializer.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serializer.StringSerializer")

    // 定义一个Kakfa Producer
    val producer = new KafkaProducer[String, String](properties)

    // 从文件中读取数据
    val bufferSource = io.Source.fromFile("")

    for(line <- bufferSource.getLines()){
      val recode = new ProducerRecord[String, String](topic, line)
      producer.send(recode)
    }

    producer.close()
  }

}
