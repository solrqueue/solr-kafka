package com.github.solrqueue.solrkafka;

import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

public class TestKafkaClientFactory extends KafkaClientFactory {
  public AdminClient adminClient;
  public Producer producer;
  public Consumer consumer;

  public TestKafkaClientFactory() {}

  public TestKafkaClientFactory(AdminClient client) {
    this.adminClient = client;
  }

  public TestKafkaClientFactory(Consumer client) {
    this.consumer = client;
  }

  @Override
  public AdminClient adminClient(Properties props) {
    return adminClient;
  }

  public <K, V> Consumer<K, V> consumer(Properties props) {
    return consumer;
  }

  public <K, V> Producer<K, V> producer(Properties props) {
    return producer;
  }
}
