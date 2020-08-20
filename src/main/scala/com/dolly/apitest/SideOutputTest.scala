package com.dolly.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 *
 * @author yusenyang 
 * @create 2020/8/16 19:37
 */
object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost", 7777)

    val dataStream: DataStream[SensorReading] = stream
      .map(data => {
        val dataArray = data.split(',')
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      })

    val processStream = dataStream
      .keyBy(_.id)
      .process(new FreezingAlert)

//    dataStream.print("input data")
    processStream.print("processed data")
    processStream.getSideOutput(new OutputTag[String]("freezing alert")).print("alert data")

    env.execute("side output test")
  }
}

// 冰点报警，如果小于32F，输出报警信息到侧输出流
class FreezingAlert() extends ProcessFunction[SensorReading, SensorReading] {

  lazy val alertOutput: OutputTag[String] = new OutputTag[String]("freezing alert")

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {

    if (value.temperature < 32.0) {
      // 进入侧输出流
      ctx.output(alertOutput, "freezing alert for " + value.id)
    } else {
      // 进入主流
      out.collect(value)
    }
  }
}