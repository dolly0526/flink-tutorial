package com.github.dolly0526.flink.tutorial.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 *
 * @author yusenyang 
 * @create 2020/8/15 18:57
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.getConfig.setAutoWatermarkInterval(100L)

//    val stream = env.readTextFile("/Users/sgcx017/github/flink-tutorial/src/main/resources/sensor.txt")
    val stream = env.socketTextStream("localhost", 7777)

    val dataStream: DataStream[SensorReading] = stream
      .map(data => {
        val dataArray = data.split(',')
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
//      .assignAscendingTimestamps(_.timestamp * 1000)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      })

    // 统计15秒内的最小温度
    val minTempPerWindowStream = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15), Time.seconds(5)) // 开窗
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2))) // 增量聚合

    minTempPerWindowStream.print("min temp")
    dataStream.print("input data")

    dataStream
      .keyBy(_.id)
      .process(new MyProcess)

    env.execute("window test")
  }
}

//class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {
//  val bound = 60000
//  var maxTs = Long.MinValue
//
//  override def getCurrentWatermark: Watermark = {
//    new Watermark(maxTs - bound)
//  }
//
//  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
//    val ts = element.timestamp * 1000
//    maxTs = maxTs.max(ts)
//    ts
//  }
//}

//class MyAssigner() extends AssignerWithPunctuatedWatermarks[SensorReading] {
//  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
//    new Watermark(extractedTimestamp)
//  }
//
//  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
//    element.timestamp * 1000
//  }
//}

class MyProcess() extends KeyedProcessFunction[String, SensorReading, String] {
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    ctx.timerService().registerEventTimeTimer(2000)
  }
}
