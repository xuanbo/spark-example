package com.example.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

/**
 * word count程序
 *
 * bash:
 * bin/spark-submit --class "com.example.spark.rdd.WordCountApp" spark-example-1.0-SNAPSHOT.jar /word.txt /word-output
 *
 * @author xuan
 * @since 1.0.0
 */
public class WordCountApp {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountApp.class);

    public static void main(String[] args) {
        // The first thing a Spark program must do is to create a JavaSparkContext object
        // which tells Spark how to access a cluster.
        SparkConf conf = new SparkConf()
                .setAppName(WordCountApp.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        // input output
        String readPath = args[0];
        String savePath = args[1];
        LOG.info("wordcount read file path: {}", readPath);
        LOG.info("wordcount save file path: {}", savePath);

        // rdd operations
        sc.textFile(readPath)
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .map(tuple -> tuple._1 + ": " + tuple._2)
                .saveAsTextFile(savePath);
    }

}
