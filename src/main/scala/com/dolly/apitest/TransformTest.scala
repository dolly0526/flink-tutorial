package com.dolly.apitest

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

/**
 *
 * @author yusenyang 
 * @create 2020/8/7 13:18
 */
object TransformTest {
  def main(args: Array[String]): Unit = {
    // environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // source
    val streamFromFile = env.readTextFile("/Users/sgcx017/github/flink-tutorial/src/main/resources/sensor.txt")

    // 1. 基本转换算子和简单聚合算子
    val dataStream: DataStream[SensorReading] = streamFromFile
      .map(data => {
        val dataArray = data.split(',')
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
      .keyBy(_.id)
      .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))

    // 2. 多流转换算子
    // 2.1 分流
    val splitStream = dataStream
      .split(data => {
        if (data.temperature > 30) Seq("high")
        else Seq("low")
      })
    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all = splitStream.select("high", "low")

    // 2.2 合并两条流
    val warning = high.map(data => (data.id, data.temperature))
    val connectedStream = warning.connect(low)
    val coMapDataStream = connectedStream.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )

    val unionStream = high.union(low).union(all)

    // 3. UDF函数类
    dataStream.filter(new MyFilter).print()

    // sink
//    dataStream.print()
//    high.print("high")
//    low.print("low")
//    all.print("all")
//    coMapDataStream.print("coMap")
    // execute
    env.execute("transform test")
  }
}

class MyFilter() extends FilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("sensor_1")
  }
}

//class MyMapper() extends RichMapFunction[SensorReading] {
//  override def map(in: SensorReading): String = {
//    "flink"
//  }
//}