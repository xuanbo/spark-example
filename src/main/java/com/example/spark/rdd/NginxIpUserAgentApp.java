package com.example.spark.rdd;

import com.example.spark.util.useragent.UserAgentParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * 基于nginx访问日志，根据ip分析用户浏览器数量
 *
 * bash:
 * bin/spark-submit --class "com.example.spark.rdd.NginxIpUserAgentApp" spark-example-1.0-SNAPSHOT.jar /access.log /access-output
 *
 * @author xuan
 * @since 1.0.0
 */
public class NginxIpUserAgentApp {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(WordCountApp.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        String readPath = args[0];
        String savePath = args[1];

        sc.textFile(readPath)
                // 转化为`浏览器:ip`的形式
                .map(line -> getBrowserFromLine(line) + ":" + getIpFromLine(line))
                // 去重
                .distinct()
                // 每出现1次，浏览器计为1
                .mapToPair(browserIp -> new Tuple2<>(browserIp.split(":")[0], 1))
                // 浏览器value累加
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
