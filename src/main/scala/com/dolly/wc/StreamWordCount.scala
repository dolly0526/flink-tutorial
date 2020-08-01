package com.dolly.wc

import org.apache.flink.streaming.api.scala._

/**
 * 流处理word count程序
 *
 * @author yusenyang 
 * @create 2020/8/1 18:20
 */
object StreamWordCount {

  def main(args: Array[String]): Unit = {
    // 创建一个执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收一个socket文本流
    val dataStream = env.socketTextStream("localhost", 7777)

    // 对每条数据进行处理
    val wordCountDataStream = dataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)

    // 打印结果
    wordCountDataStream.print()

    // 启动executor
    // nc -lk 7777
    env.execute("stream word count job")
  }
}
