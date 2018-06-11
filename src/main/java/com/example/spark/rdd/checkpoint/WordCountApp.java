package com.example.spark.rdd.checkpoint;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

/**
 * word count程序
 *
 * bash:
 * bin/spark-submit --class "com.example.spark.rdd.checkpoint.WordCountApp" spark-example-1.0-SNAPSHOT.jar /word.txt /word-output
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
        JavaRDD<String> result = sc.textFile(readPath)
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                // Shuffled
                .reduceByKey((a, b) -> a + b)
                .map(tuple -> tuple._1 + ": " + tuple._2);

        // 设置checkpoint目录
        sc.setCheckpointDir("hdfs://hadoop0:9000/checkpoint");

        // 先cache，checkpoint时直接用cache的结果
        result.cache();

        // checkpoint
        // checkpoint会触发一个Job,如果执行checkpoint的rdd是由其他rdd经过许多计算转换过来的
        // 如果你没有persisted这个rdd，那么又要重头开始计算该rdd，也就是做了重复的计算工作了
        // 所以建议先persist rdd然后再checkpoint，checkpoint会丢弃该rdd的以前的依赖关系，使该rdd成为顶层父rdd
        // 这样在失败的时候恢复只需要恢复该rdd,而不需要重新计算该rdd了
        result.checkpoint();

        // save
        result.saveAsTextFile(savePath);

        // Stop
        sc.stop();
    }

}
