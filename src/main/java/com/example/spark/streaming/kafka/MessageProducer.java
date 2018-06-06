package com.example.spark.streaming.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 消息生产者
 *
 * @author xuan
 * @since 1.0.0
 */
public class MessageProducer {

    public static void main(String[] args) {
        // 配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "47.104.131.255:9092");
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Send
        producer.send(new ProducerRecord<>("test-kafka", "hello", "hello"));
        producer.send(new ProducerRecord<>("test-kafka", "hello", "hello"));
        producer.send(new ProducerRecord<>("test-kafka", "world", "world"));
        producer.send(new ProducerRecord<>("test-kafka", "world", "world"));
        producer.send(new ProducerRecord<>("test-kafka", "spring", "spring"));
        producer.send(new ProducerRecord<>("test-kafka", "spring", "spring"));

        // Close
        producer.close();
    }

}
