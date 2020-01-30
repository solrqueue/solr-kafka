package com.github.solrqueue.solrkafka;

import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

/**
 * Factory for kafka admin, consumer, and producer clients. Allows override with mock clients in
 * test
 */
public class KafkaClientFactory {

  /**
   * get a new KafkaAdminClient
   *
   * @param props configuration to pass to KafkaAdminClient.create
   * @return new KafkaAdminClient
   */
  public AdminClient adminClient(Properties props) {
    return KafkaAdminClient.create(props);
  }

  /**
   * get a new KafkaConsumer
   *
   * @param props configuration to pass to KafkaConsumer constructor
   * @param <K> record key type
   * @param <V> record value type
   * @return new KafkaConsumer
   */
  public <K, V> Consumer<K, V> consumer(Properties props) {
    return new KafkaConsumer<>(props);
  }

  /**
   * get a new KafkaProducer
   *
   * @param props configuration to pass to KafkaProducer constructor
   * @param <K> record key type
   * @param <V> record value type
   * @return new KafkaProducer
   */
  public <K, V> Producer<K, V> producer(Properties props) {
    return new KafkaProducer<>(props);
  }

  void setFactory(KafkaClientFactory factory) {
    INSTANCE = factory;
  }

  protected KafkaClientFactory() {}

  public static KafkaClientFactory INSTANCE = new KafkaClientFactory();
}
