package com.dolly.apitest

import java.util.{Properties, Random}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 *
 * @author yusenyang 
 * @create 2020/8/6 12:28
 */
object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 从自定义的集合中读取数据
    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))

//    env.fromElements(1, 2.0, "string").print()


    // 2. 从文件中读取数据
    val stream2 = env.readTextFile("/Users/sgcx017/github/flink-tutorial/src/main/resources/sensor.txt")


    // 3. 从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))


    // 4. 自定义source
    val stream4 = env.addSource(new SensorSource())


    stream4.print("stream4").setParallelism(2)
    env.execute("source test")
  }
}

// 定义样例类，传感器 id，时间戳，温度
case class SensorReading(id: String, timestamp: Long, temperature: Double)

class SensorSource() extends SourceFunction[SensorReading] {

  // flag: 表示数据源是否还在正常运行
  var running: Boolean = true

  // 正常生成数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

    // 初始化一个随机数发生器
    val rand = new Random()

    // 初始化定义一组传感器温度数据
    var curTemp = 1.to(10).map(
      i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
    )

    // 死循环产生数据流
    while (running) {
      // 在前一次温度的基础上更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian())
      )

      // 获取当前时间戳
      val curTime = System.currentTimeMillis()

      curTemp.foreach(
        t => ctx.collect(SensorReading(t._1, curTime, t._2))
      )

      // 设置时间间隔
      Thread.sleep(500)
    }
  }

  // 取消数据源的生成
  override def cancel(): Unit = {
    running = false
  }
}