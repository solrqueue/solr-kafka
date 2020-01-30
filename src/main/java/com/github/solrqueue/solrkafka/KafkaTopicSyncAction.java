package com.github.solrqueue.solrkafka;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.autoscaling.ActionContext;
import org.apache.solr.cloud.autoscaling.TriggerActionBase;
import org.apache.solr.cloud.autoscaling.TriggerEvent;
import org.apache.solr.cloud.autoscaling.TriggerValidationException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TriggerAction for synchronizing SolrCloud configuration (list of collections) with Kafka
 * configuration (list of topics) Designed to be used a an action called by a "scheduled" trigger:
 *
 * <pre>
 * {
 *     "name" : "kafka_topic_sync",
 *     "startTime": "NOW+10SECONDS",
 *     "event" : "scheduled",
 *     "every" : "+10SECONDS",
 *     "graceDuration" : "+1YEAR",  (we don't care how old the event is)
 *     "enabled" : true,
 *     "actions" : [{
 *         "name" : "sync_action",
 *         "class": "com.github.solrqueue.solrkafka.KafkaTopicSyncAction",
 *         "bootstrap.servers": "localhost:9093,localhost:9094",
 *         "replication.factor": 2
 *     }]
 * }
 * </pre>
 */
public class KafkaTopicSyncAction extends TriggerActionBase {
  private static final String BOOTSTRAP_SERVERS = AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
  private static final String REPLICATION_FACTOR = "replication.factor";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private String bootstrapServers;
  private short replicationFactor = 2;

  /**
   * Called each time the trigger runs, used here as a param validation hook.
   *
   * @param loader loader to use for instantiating sub-components (unused)
   * @param cloudManager current instance of SolrCloudManager (unused)
   * @param properties configuration properties, which are validated
   * @throws TriggerValidationException contains details of invalid configuration parameters.
   */
  @Override
  public void configure(
      SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> properties)
      throws TriggerValidationException {
    // This is called every time the trigger runs, not just once per boot
    validProperties.add(BOOTSTRAP_SERVERS);
    validProperties.add(REPLICATION_FACTOR);
    requiredProperties.add(BOOTSTRAP_SERVERS);
    super.configure(loader, cloudManager, properties);
    bootstrapServers = (String) properties.get(BOOTSTRAP_SERVERS);
    Object replicationFactorConfig = properties.get(REPLICATION_FACTOR);
    if (replicationFactorConfig != null) {
      replicationFactor = Short.parseShort(replicationFactorConfig.toString());
    }
  }

  /**
   * Called each time the trigger runs. Compare the Solr and Kafka cluster state
   *
   * @param event event object (unused)
   * @param context context (access to Solr cluster state)
   * @throws Exception if the kafka client encounters an error
   */
  @Override
  public void process(TriggerEvent event, ActionContext context) throws Exception {
    log.debug(
        "-- processing event: {} with context properties: {}", event, context.getProperties());
    Map<String, DocCollection> collectionsMap =
        context.getCloudManager().getClusterStateProvider().getClusterState().getCollectionsMap();
    Properties props = new Properties();
    props.put(BOOTSTRAP_SERVERS, bootstrapServers);
    AdminClient kafkaClient = KafkaClientFactory.INSTANCE.adminClient(props);
    Set<String> topicNames = kafkaClient.listTopics().names().get();

    Set<String> topicsToCreate = new HashSet<>(collectionsMap.keySet());
    topicsToCreate.removeAll(topicNames);

    Set<String> topicsToDelete = new HashSet<>(topicNames);
    topicsToDelete.removeAll(collectionsMap.keySet());

    if (!topicsToCreate.isEmpty()) {
      List<NewTopic> newTopics = new ArrayList<>();
      for (String collectionName : topicsToCreate) {
        int partitions = collectionsMap.get(collectionName).getSlices().size();
        log.info("Creating topic {} with {} partitions", collectionName, partitions);
        newTopics.add(new NewTopic(collectionName, partitions, replicationFactor));
      }
      kafkaClient.createTopics(newTopics).all().get();
    }

    if (!topicsToDelete.isEmpty()) {
      log.info("Deleting topics: {}", Arrays.toString(topicsToDelete.toArray()));
      kafkaClient.deleteTopics(topicsToDelete).all().get();
    }
    kafkaClient.close();
  }
}
