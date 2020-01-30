package com.github.solrqueue.solrkafka.integration;

import static org.apache.solr.common.params.CommonParams.JSON_MIME;

import com.github.solrqueue.solrkafka.KafkaClientFactory;
import com.yammer.metrics.Metrics;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.solr.SolrTestCaseJ4.SuppressObjectReleaseTracker;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.LogLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

@SuppressObjectReleaseTracker(bugUrl = "leaks no objects in single test")
@LogLevel("com.github.solrqueue.solrkafka=DEBUG")
public class TestSolrKafkaDistrib extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static Path SOLR_HOME = Paths.get("src/test/resources/solr_home");
  protected static KafkaServer kafkaServer = null;
  protected static String kafkaBootstrapServerHostPort = null;

  public static final String TRIGGER_TMPL =
      ""
          + "{\n"
          + " \"set-trigger\": {\n"
          + "  \"name\" : \"kafka_topic_sync\",\n"
          + "  \"startTime\": \"NOW\",\n"
          + "  \"event\" : \"scheduled\",\n"
          + "  \"every\" : \"+1SECONDS\",\n"
          + "  \"graceDuration\" : \"+1YEAR\",\n"
          + "  \"enabled\" : true,\n"
          + "  \"actions\" : [\n"
          + "   {\n"
          + "    \"name\" : \"sync_action\",\n"
          + "    \"class\": \"com.github.solrqueue.solrkafka.KafkaTopicSyncAction\",\n"
          + "    \"bootstrap.servers\": \"%1$s\",\n"
          + "    \"replication.factor\": %2$s\n"
          + "   }\n"
          + "  ]\n"
          + " }\n"
          + "}\n";

  public static final String DELETE_TRIGGER =
      "{\"remove-trigger\": {\"name\": \"kafka_topic_sync\"}}";

  private static class AutoscalingRequest extends SolrRequest {
    private final String json;

    AutoscalingRequest(String json) {
      super(METHOD.POST, "/admin/autoscaling");
      this.json = json;
    }

    @Override
    public SolrParams getParams() {
      return new ModifiableSolrParams();
    }

    @Override
    public RequestWriter.ContentWriter getContentWriter(String expectedType) {
      return new RequestWriter.StringPayloadContentWriter(json, JSON_MIME);
    }

    @Override
    protected SolrResponse createResponse(SolrClient client) {
      return new SolrResponseBase();
    }
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).configure();
    // Kafka creates a bunch of watches.  Don't alert on high number of watches.
    cluster.getZkServer().getLimiter().setLimit(100);

    CloudSolrClient cloudClient = cluster.getSolrClient();
    ZkClientClusterStateProvider provider =
        (ZkClientClusterStateProvider) cloudClient.getClusterStateProvider();

    Properties kafkaServerProps = new Properties();
    Path logDir = Files.createTempDirectory("kafka_test_log");
    // Using Port 0 means that the port is auto-assigned.
    kafkaServerProps.put("listeners", SecurityProtocol.PLAINTEXT + "://localhost:0");
    kafkaServerProps.put("auto.create.topics.enable", "false");
    kafkaServerProps.put("zookeeper.connect", cloudClient.getZkHost());
    kafkaServerProps.put("log.dirs", logDir.resolve("data").toAbsolutePath().toString());
    kafkaServerProps.put("zookeeper.connection.timeout.ms", "10000");
    // We don't track offsets in kafka, but this suppresses an annoying log message
    kafkaServerProps.put("offsets.topic.replication.factor", "1");
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    FileUtils.deleteDirectory(logDir.toFile());
                  } catch (IOException ioe) {
                    log.error("Failed to clean up logDir", ioe);
                  }
                }));

    KafkaConfig config = KafkaConfig.fromProps(kafkaServerProps);
    kafkaServer =
        new KafkaServer(
            config,
            Time.SYSTEM,
            Option.empty(),
            scala.collection.JavaConversions.asScalaBuffer(Collections.emptyList()));
    kafkaServer.startup();
    kafkaBootstrapServerHostPort =
        "localhost:"
            + kafkaServer.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT));

    System.setProperty("kafkaBroker", kafkaBootstrapServerHostPort);
    provider.uploadConfig(SOLR_HOME.resolve("testcore_kafka").resolve("conf"), "conf1");
  }

  @Test
  public void test() throws Exception {
    cluster.waitForAllNodes(5000);

    String collection = "solr_kafka1";
    int numShards = 2;

    CloudSolrClient cloudClient = cluster.getSolrClient();
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServerHostPort);
    AdminClient kafkaClient = KafkaClientFactory.INSTANCE.adminClient(props);
    assertTrue("start with no collections", kafkaClient.listTopics().listings().get().isEmpty());

    // Add the collection scheduled trigger
    String addTriggerJson = String.format(TRIGGER_TMPL, kafkaBootstrapServerHostPort, 1);
    SolrRequest addTriggerRequest = new AutoscalingRequest(addTriggerJson);

    NamedList<Object> res = cloudClient.request(addTriggerRequest);
    // NOTE: per the response message, the trigger response structure may change
    // so this test may brake in the future.  for now, we just want to ensure
    // that we get some kind of successful result
    assertEquals("trigger json result is success", "success", res.get("result"));
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collection, "conf1", numShards, 1);
    create.setMaxShardsPerNode(numShards - 1);
    cloudClient.request(create);

    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    DocCollection coll = clusterState.getCollection(collection);

    assertEquals(collection, coll.getName());

    // Wait for kafka trigger to run
    Thread.sleep(2000);

    Map<String, TopicListing> listings = kafkaClient.listTopics().namesToListings().get();
    assertEquals("has 1 topic for the collection", 1, listings.size());
    assertTrue("topic name is collection name", listings.containsKey(collection));
    Map<String, TopicDescription> desc =
        kafkaClient.describeTopics(Collections.singletonList(collection)).all().get();
    assertEquals(
        "topic partitions is shard num", numShards, desc.get(collection).partitions().size());

    for (String id : new String[] {"test1", "test2", "test3"}) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id", id);
      UpdateResponse updateResponse = cloudClient.add(collection, doc, 100);
      System.out.println("add " + id + " update res " + updateResponse);
    }

    ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.add("q", "*:*");
    QueryResponse qRes = null;
    for (int tries = 0; tries < 10; tries++) {
      qRes = cloudClient.query(collection, queryParams);
      if (qRes.getResults().getNumFound() == 3) break;
      Thread.sleep(1000);
      System.out.println("qRes.getResults().getNumFound() " + qRes.getResults().getNumFound());
    }

    assertEquals("all docs in index", 3, qRes.getResults().getNumFound());

    cloudClient.request(CollectionAdminRequest.deleteCollection(collection));

    // Wait for kafka trigger to run
    Thread.sleep(5000);

    listings = kafkaClient.listTopics().namesToListings().get();
    assertEquals("no topics after collection deleted", 0, listings.size());

    // Cleaning up the trigger prevents leaks
    SolrRequest deleteTriggerRequest = new AutoscalingRequest(DELETE_TRIGGER);
    cloudClient.request(deleteTriggerRequest);

    kafkaClient.close();
  }

  @AfterClass
  public static void shutdownKafka() throws Exception {
    System.clearProperty("kafkaBroker");
    kafkaBootstrapServerHostPort = null;
    if (kafkaServer != null) {
      kafkaServer.shutdown();
      kafkaServer.metrics().close();
      // prevents leaking "metrics-meter-tick-thread" threads
      Metrics.shutdown();
    }
  }
}
