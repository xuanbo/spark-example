package com.example.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * 基于nginx访问日志，获取每个ip访问的次数
 *
 * bash:
 * bin/spark-submit --class "com.example.spark.rdd.NginxIpApp" spark-example-1.0-SNAPSHOT.jar /access.log /access-output
 *
 * @author xuan
 * @since 1.0.0
 */
public class NginxIpApp {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(WordCountApp.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        String readPath = args[0];
        String savePath = args[1];

        sc.textFile(readPath)
                // (ip1, 1)、(ip1, 1)、(ip2, 1)...
                .mapToPair(line -> new Tuple2<>(getIpFromLine(line), 1))
                // (ip1, 2)、(ip2, 1)...
                .reduceByKey((a, b) -> a + b)
                .map(tuple -> tuple._1 + ": " + tuple._2)
                .saveAsTextFile(savePath);
    }

    /**
     * 获取ip
     *
     * @param line 一行日志信息
     * @return ip
     */
    private static String getIpFromLine(String line) {
        String[] split = line.split(" ");
        return split[0];
    }

}
