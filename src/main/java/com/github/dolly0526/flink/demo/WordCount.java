package com.github.dolly0526.flink.demo;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author yusenyang
 * @create 2021/3/16 14:25
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String inputPath = "/Users/sgcx017/github/flink-demo/flink-tutorial/src/main/resources/hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        // 空格分词打散之后，对单词进行 groupby 分组，然后用 sum 进行聚合
        DataSet<Tuple2<String, Integer>> wordCountDataSet = inputDataSet
                .flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);

        // 打印输出
        wordCountDataSet.print();
    }
}
