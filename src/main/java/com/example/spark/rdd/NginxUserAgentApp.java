package com.example.spark.rdd;

import com.example.spark.util.useragent.UserAgentParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * 基于nginx访问日志，分析用户浏览器数量
 *
 * bash:
 * bin/spark-submit --class "com.example.spark.rdd.NginxUserAgentApp" spark-example-1.0-SNAPSHOT.jar /access.log /access-output
 *
 * @author xuan
 * @since 1.0.0
 */
public class NginxUserAgentApp {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(WordCountApp.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        String readPath = args[0];
        String savePath = args[1];

        sc.textFile(readPath)
                // 将一行ngxin的access.log获取到浏览器信息
                .map(NginxUserAgentApp::getBrowserFromLine)
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .map(tuple -> tuple._1 + ": " + tuple._2)
                .saveAsTextFile(savePath);
    }

    /**
     * 获取浏览器
     *
     * @param line 一行日志信息
     * @return user-agent中提取的浏览器信息
     */
    private static String getBrowserFromLine(String line) {
        return new UserAgentParser()
                .parse(userAgentExtract(line))
                .getBrowser();
    }

    /**
     * 根据自己的日志编写提取user-agent字符串规则
     *
     * @param line 一行日志信息
     * @return user-agent字符串
     */
    private static String userAgentExtract(String line) {
        String[] split = line.split("\"");
        int length = split.length;
        return split[length - 1];
    }

}
