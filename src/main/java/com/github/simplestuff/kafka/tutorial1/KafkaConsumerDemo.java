package com.github.simplestuff.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerDemo {

  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(KafkaConsumerDemo.class.getName());
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "auto-java-app-1");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    consumer.subscribe(Arrays.asList("first_topic"));
    /*
    TopicPartition  partition = new TopicPartition("first_topic", 0);
    consumer.assign(Collections.singleton(partition));
     */

    while (true) {
      ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(3000));

      for (ConsumerRecord record: records) {
        logger.info("Received key: " + record.key() + " value: " + record.value() + " from partition: "
            + record.partition());
      }
    }
  }
}
