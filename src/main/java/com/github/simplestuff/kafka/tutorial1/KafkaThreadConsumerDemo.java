package com.github.simplestuff.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class KafkaThreadConsumerDemo {

  private static class ThreadRunner implements Runnable {

    private CountDownLatch latch;

    private Logger logger;

    private KafkaConsumer<String, String> consumer;

    public ThreadRunner(Properties properties, CountDownLatch latch, Logger logger, String topic) {
      this.latch = latch;
      this.logger = logger;
      this.consumer = new KafkaConsumer<>(properties);
      this.consumer.subscribe(Collections.singleton(topic));
    }

    @Override
    public void run() {
      try {
        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

          for (ConsumerRecord record : records) {
            logger.info("Received key: " + record.key() + " value: " + record.value() + " from partition: "
                    + record.partition());
          }
        }
      } catch (WakeupException exception) {
        logger.error("Interrupted: ", exception);
      } finally {
        this.consumer.close();
        logger.info("Closing consumer");
        this.latch.countDown();
        logger.info("Releasing lock");
      }
    }

    private void shutDown() {
      this.consumer.wakeup();
    }
  }



  public static void main(String[] args) {
    new KafkaThreadConsumerDemo().run();
  }

  public void run() {
    Logger logger = LoggerFactory.getLogger(KafkaThreadConsumerDemo.class.getName());
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "auto-java-app-1");

    CountDownLatch latch = new CountDownLatch(1);
    ThreadRunner runner1 = new ThreadRunner(properties, latch, logger, "first_topic");

    new Thread(runner1).start();
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        runner1.shutDown();

        try {
          latch.await();
        } catch (InterruptedException exception) {
        } finally {
          logger.info("Main sub thread exit");
        }
      }
    }));

    try {
      latch.await();
    } catch (InterruptedException exception) {
      exception.printStackTrace();
    } finally {
      logger.info("Shut down main complete");
      System.out.println("Shutting down now");
    }
  }
}
