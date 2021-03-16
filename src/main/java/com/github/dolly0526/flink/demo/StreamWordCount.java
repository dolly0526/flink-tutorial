package com.github.dolly0526.flink.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yusenyang
 * @create 2021/3/16 14:25
 */
public class StreamWordCount {

    public static final String host = "localhost";
    public static final int port = 7777;


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        DataStream<Tuple2<String, Integer>> wordCountDataStream = inputDataStream
                .flatMap(new MyFlatMapper())
                .keyBy(0)
                .sum(1);

        wordCountDataStream.print().setParallelism(1);

        env.execute();
    }
}
