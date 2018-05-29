package com.example.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

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

        ssc.textFileStream("file:///usr/local/spark/text-input")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .map(tuple -> tuple._1 + ": " + tuple._2)
                .print();

        ssc.start();

        ssc.awaitTermination();
    }

}
