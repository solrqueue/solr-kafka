package com.github.solrqueue.solrkafka;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.cloud.autoscaling.ActionContext;
import org.apache.solr.cloud.autoscaling.TriggerEvent;
import org.apache.solr.cloud.autoscaling.TriggerValidationException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.junit.Assert;
import org.junit.Test;

public class KafkaTopicSyncActionTest extends SingleCoreTestBase {

  @Test
  public void testConfigure() throws Exception {
    KafkaTopicSyncAction syncAction = new KafkaTopicSyncAction();

    try {
      syncAction.configure(getTestCore().getResourceLoader(), null, Collections.emptyMap());
      Assert.fail("Expected configure exception without required params");
    } catch (TriggerValidationException tve) { // expected
    }

    syncAction.configure(
        getTestCore().getResourceLoader(),
        null,
        Collections.singletonMap("bootstrap.servers", "localhost:9000"));
  }

  @Test
  public void testProcess() throws Exception {
    MockAdminClient mockAdminClient = new MockAdminClient();
    mockAdminClient.topicList = new HashMap<>();
    KafkaClientFactory.INSTANCE = new TestKafkaClientFactory(mockAdminClient);

    KafkaTopicSyncAction syncAction = new KafkaTopicSyncAction();
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("bootstrap.servers", "localhost:9000");
    configMap.put("replication.factor", "11");
    syncAction.configure(getTestCore().getResourceLoader(), null, configMap);

    TestSolrCloudManager cm = new TestSolrCloudManager();
    TestClusterStateProvider csp = new TestClusterStateProvider();

    // Start with no collections to create/delete
    Map<String, DocCollection> collectionMap = new HashMap<>();
    csp.clusterState = new ClusterState(1, Collections.emptySet(), collectionMap);
    cm.clusterStateProvider = csp;

    syncAction.process(
        new TriggerEvent(
            TriggerEventType.SCHEDULED, "test", System.currentTimeMillis(), Collections.emptyMap()),
        new ActionContext(cm, null, Collections.emptyMap()));
    assertEquals(0, mockAdminClient.createdTopics);
    assertEquals(0, mockAdminClient.deletedTopics);

    // Delete 2 collections, t1, t2
    mockAdminClient.topicList.put("t1", new TopicListing("t1", false));
    mockAdminClient.topicList.put("t2", new TopicListing("t2", false));

    syncAction.process(
        new TriggerEvent(
            TriggerEventType.SCHEDULED, "test", System.currentTimeMillis(), Collections.emptyMap()),
        new ActionContext(cm, null, Collections.emptyMap()));
    assertEquals(0, mockAdminClient.createdTopics);
    assertEquals(2, mockAdminClient.deletedTopics);

    // Create t1 (t2 already exists)
    mockAdminClient.topicList.remove("t1");
    Map<String, Slice> slices = new HashMap<>();
    slices.put("shard1", new Slice("shard1", null, null));
    slices.put("shard2", new Slice("shard1", null, null));
    collectionMap.put("t1", new DocCollection("t1", slices, null, null));
    collectionMap.put("t2", new DocCollection("t1", slices, null, null));
    csp.clusterState = new ClusterState(1, Collections.emptySet(), collectionMap);

    syncAction.process(
        new TriggerEvent(
            TriggerEventType.SCHEDULED, "test", System.currentTimeMillis(), Collections.emptyMap()),
        new ActionContext(cm, null, Collections.emptyMap()));
    assertEquals(1, mockAdminClient.createdTopics);
    assertEquals(2, mockAdminClient.createdPartitions);
    assertEquals(22, mockAdminClient.createdReplicas);
    assertEquals(2, mockAdminClient.deletedTopics);
  }
}
