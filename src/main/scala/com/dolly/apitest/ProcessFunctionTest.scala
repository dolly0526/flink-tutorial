package com.dolly.apitest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 *
 * @author yusenyang 
 * @create 2020/8/15 22:03
 */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    env.setStateBackend(new MemoryStateBackend())

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
        .process(new TempIncreAlert)

    val processStream2 = dataStream
        .keyBy(_.id)

    dataStream.print("input data")
    processStream.print("processed data")

    env.execute("process function test")
  }
}

class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String] {

  // 定义一个状态，用来保存上一个数据的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  // 定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {

    // 先取出上一个温度值
    val preTemp = lastTemp.value()
    // 更新温度值
    lastTemp.update(value.temperature)

    val curTimerTs = currentTimer.value()

    // 温度上升，且没有设过定时器，则注册定时器
    if (value.temperature > preTemp && curTimerTs == 0) {

      // 观察10s内的状态
      val timerTs = ctx.timerService().currentProcessingTime() + 10000L
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(timerTs)

    } else if (preTemp > value.temperature || preTemp == 0.0) {

      // 如果温度下降，或是第一条数据，删除定时器，并清空状态
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimer.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {

    // 输出报警信息
    out.collect(ctx.getCurrentKey + ": 温度连续上升")
    currentTimer.clear()
  }
}
