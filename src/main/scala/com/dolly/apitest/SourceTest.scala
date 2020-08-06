package com.dolly.apitest

import org.apache.flink.streaming.api.scala._

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
//    stream1.print("stream1").setParallelism(4)

//    env.fromElements(1, 2.0, "string").print()

    // 2. 从文件中读取数据
    val stream2 = env.readTextFile("/Users/sgcx017/github/flink-tutorial/src/main/resources/sensor.txt")
//    stream2.print("stream2").setParallelism(1)

    // 3. 从kafka中读取数据
    

    env.execute()
  }
}

// 定义样例类，传感器 id，时间戳，温度
case class SensorReading(id: String, timestamp: Long, temperature: Double)