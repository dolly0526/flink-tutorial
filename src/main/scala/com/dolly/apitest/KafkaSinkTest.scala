package com.dolly.apitest

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
 *
 * @author yusenyang 
 * @create 2020/8/9 15:41
 */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream = env.readTextFile("/Users/sgcx017/github/flink-tutorial/src/main/resources/sensor.txt")

    val dataStream = inputStream
        .map(data => {
          val dataArray = data.split(',')
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString
        })

    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092", "sink kafka test", new SimpleStringSchema()))
    dataStream.print()

    env.execute("kafka sink test")
  }
}
