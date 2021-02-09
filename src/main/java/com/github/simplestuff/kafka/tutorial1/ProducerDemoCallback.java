package com.github.simplestuff.kafka.tutorial1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoCallback {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final Logger logger = LoggerFactory.getLogger(ProducerDemoCallback.class);
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    final List<Integer> partitions = new ArrayList<>();
    for (int ii = 0; ii < 10; ii++) {
      final int finalIi = ii;
      String key = "key_id_" + ii + ii;
      producer.send(new ProducerRecord<String, String>("first_topic", key,
                      "value sent now " + ii)
              , new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                  if (e == null) {
                    synchronized (this) {
                      partitions.add(recordMetadata.partition());
                    }
                    logger.info("Received new metadata: " + recordMetadata.toString());
                  } else {
                    logger.error("Error in " + finalIi, e);
                  }
                }
              }).get();
    }
    logger.info("Partitions: " + partitions.toString());
    producer.flush();
    producer.close();
  }
}
