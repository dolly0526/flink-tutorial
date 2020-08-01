package com.dolly.wc

import org.apache.flink.api.scala._

/**
 * 批处理word count程序
 * @author yusenyang 
 * @create 2020/8/1 18:06
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    // 创建一个执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val inputPath = "/Users/sgcx017/github/flink-tutorial/src/main/resources/hello.txt"
    val inputDataSet = env.readTextFile(inputPath)

    // 切分数据得到word，然后再按word做分组聚合
    val wordCountDataSet = inputDataSet
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    // 打印结果
    wordCountDataSet.print()
  }
}
