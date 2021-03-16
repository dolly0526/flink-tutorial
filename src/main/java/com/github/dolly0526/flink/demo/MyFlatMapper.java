package com.github.dolly0526.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author yusenyang
 * @create 2021/3/16 14:35
 */
public class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

        // 按空格切分
        for (String word : value.split(" ")) {
            out.collect(new Tuple2<>(word, 1));
        }
    }
}
