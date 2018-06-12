package com.example.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 从文件中获取流
 *
 * bash:
 * bin/spark-submit --class "com.example.spark.streaming.WordCountStreamingApp" spark-example-1.0-SNAPSHOT.jar
 *
 * @author xuan
 * @since 1.0.0
 */
public class WordCountStreamingApp {

    public static void main(String[] args) throws InterruptedException {
        // Create a StreamingContext
        SparkConf conf = new SparkConf().setAppName(WordCountStreamingApp.class.getSimpleName());
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(10));

        // 第一点,如果要使用updateStateByKey算子,就必须设置一个checkpoint目录,开启checkpoint机制
        ssc.checkpoint("hdfs://hadoop0:9000/checkpoint");

        ssc.textFileStream("file:///usr/local/spark/text-input")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                // 每批次累加结果
                // 实际上,对于每个单词,每次batch计算的时候,都会调用这个函数
                // 第一个参数,values相当于这个batch中,这个key的新的值, 可能有多个
                // 比如一个hello,可能有2个1,(hello, 1) (hello, 1) 那么传入的是(1,1)
                // 那么第二个参数表示的是这个key之前的状态,其实泛型的参数是你自己指定的
                .updateStateByKey((List<Integer> values, Optional<Integer> state) -> {
                    // 批次累加结果
                    Integer sum = values.stream().reduce(0, Integer::sum);
                    return Optional.of(state.orElse(0) + sum);
                })
                .map(tuple -> tuple._1 + ": " + tuple._2)
                .print();

        ssc.start();

        ssc.awaitTermination();
    }

}
