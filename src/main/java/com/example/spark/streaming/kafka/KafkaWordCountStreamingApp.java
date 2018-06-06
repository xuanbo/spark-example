package com.example.spark.streaming.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 从kafka中获取流，每10秒统计一次
 *
 * bash:
 * bin/spark-submit --class "com.example.spark.streaming.kafka.KafkaWordCountStreamingApp" spark-example-1.0-SNAPSHOT.jar
 *
 * @author xuan
 * @since 1.0.0
 */
public class KafkaWordCountStreamingApp {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaWordCountStreamingApp.class);

    public static void main(String[] args) throws InterruptedException {
        // Create a StreamingContext
        SparkConf conf = new SparkConf().setAppName(KafkaWordCountStreamingApp.class.getSimpleName());
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(10));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "47.104.131.255:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", KafkaWordCountStreamingApp.class.getName());
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singletonList("test-kafka");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        stream.foreachRDD(rdd -> {
            // offsets
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

            // calculation
            rdd.map(ConsumerRecord::value)
                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey((a, b) -> a + b)
                    .map(tuple -> tuple._1 + ": " + tuple._2)
                    .foreachPartition(results -> results.forEachRemaining(data -> LOG.info("data: {}", data)));

            // Persist Offsets in Kafka
            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
        });

        ssc.start();

        ssc.awaitTermination();
    }

}
