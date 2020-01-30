package com.github.solrqueue.solrkafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.junit.Test;

public class KafkaProducerUpdateProcessorTest extends SingleCoreTestBase {
  static class TestKafkaProducerUpdateProcessor extends KafkaProducerUpdateProcessor {
    private ClusterState clusterState;

    TestKafkaProducerUpdateProcessor(
        Producer<String, byte[]> producer,
        SolrQueryRequest req,
        SolrQueryResponse rsp,
        UpdateRequestProcessor next,
        String collection) {
      super(producer, req, rsp, next, false);
      this.collection = collection;
    }

    protected ClusterState getClusterState() {
      return clusterState;
    }

    void setClusterState(ClusterState clusterState) {
      this.clusterState = clusterState;
    }
  }

  private DocCollection collectionWithTwoShards(String name) {
    Map<String, Slice> slices = new HashMap<>();
    DocRouter.Range shardRange1 = new DocRouter.Range(Integer.MIN_VALUE, -1);
    DocRouter.Range shardRange2 = new DocRouter.Range(0, Integer.MAX_VALUE);
    slices.put("shard1", new Slice("shard1", null, Collections.singletonMap("range", shardRange1)));
    slices.put("shard2", new Slice("shard2", null, Collections.singletonMap("range", shardRange2)));
    return new DocCollection(name, slices, null, new CompositeIdRouter());
  }

  @Test
  public void testProcessAdd() throws Exception {
    MockProducer<String, byte[]> producer = new MockProducer<>();
    LocalSolrQueryRequest req = emptyReq();
    TestUpdateRequestProcessor next = new TestUpdateRequestProcessor();

    SolrQueryResponse res = new SolrQueryResponse();
    TestKafkaProducerUpdateProcessor updateProcessor =
        new TestKafkaProducerUpdateProcessor(producer, req, res, next, "c1");

    DocCollection c1 = collectionWithTwoShards("c1");

    ClusterState cs =
        new ClusterState(1, Collections.emptySet(), Collections.singletonMap("c1", c1));
    updateProcessor.setClusterState(cs);

    AddUpdateCommand cmd = new AddUpdateCommand(req);
    cmd.solrDoc = new SolrInputDocument();
    cmd.solrDoc.setField("id", "test_doc_1");
    updateProcessor.processAdd(cmd);
    // manually complete the future before calling finish
    producer.completeNext();
    updateProcessor.finish();

    Slice target = new CompositeIdRouter().getTargetSlice("test_doc_1", null, null, null, c1);
    assertEquals("shard1", target.getName());
    assertEquals(1, producer.history().size());
    assertEquals(0, producer.history().get(0).partition().intValue());
    assertEquals("c1", producer.history().get(0).topic());
    assertEquals("test_doc_1", producer.history().get(0).key());
    assertEquals(0, next.processAddCount);
    assertEquals(1, res.getValues().size());
    assertNotNull(res.getValues().get("kafka"));

    // Test the behavior when no "next" is set, and no collection is set (does nothing)
    res = new SolrQueryResponse();
    updateProcessor = new TestKafkaProducerUpdateProcessor(producer, req, res, null, null);
    updateProcessor.processAdd(cmd);
    updateProcessor.finish();
    assertEquals(0, res.getValues().size());

    // Test the behavior when "next" is set, and no collection is set
    res = new SolrQueryResponse();
    updateProcessor = new TestKafkaProducerUpdateProcessor(producer, req, res, next, null);
    updateProcessor.processAdd(cmd);
    updateProcessor.finish();
    assertEquals(1, next.processAddCount);
    assertEquals(0, res.getValues().size());

    // Test the behavior when "skip" is set
    res = new SolrQueryResponse();
    updateProcessor = new TestKafkaProducerUpdateProcessor(producer, req, res, next, "c1");
    ModifiableSolrParams params = new ModifiableSolrParams(cmd.getReq().getParams());
    params.set("kafka.skip", "true");
    cmd.getReq().setParams(params);
    updateProcessor.processAdd(cmd);
    updateProcessor.finish();
    assertEquals(2, next.processAddCount);
    assertEquals(0, res.getValues().size());
  }

  @Test
  public void testProcessDelete() throws Exception {
    MockProducer<String, byte[]> producer = new MockProducer<>();
    LocalSolrQueryRequest req = emptyReq();
    TestUpdateRequestProcessor next = new TestUpdateRequestProcessor();

    TestKafkaProducerUpdateProcessor updateProcessor =
        new TestKafkaProducerUpdateProcessor(producer, req, new SolrQueryResponse(), next, "c1");

    DocCollection c1 = collectionWithTwoShards("c1");

    ClusterState cs =
        new ClusterState(1, Collections.emptySet(), Collections.singletonMap("c1", c1));
    updateProcessor.setClusterState(cs);

    int expectedDocs = 0;
    // Delete by Id
    DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
    cmd.id = "test_doc_3";
    updateProcessor.processDelete(cmd);
    Slice target = new CompositeIdRouter().getTargetSlice("test_doc_3", null, null, null, c1);
    assertEquals("shard2", target.getName());
    assertEquals(1 + expectedDocs, producer.history().size());
    expectedDocs += 1;
    assertEquals(1, producer.history().get(0).partition().intValue());
    assertEquals("c1", producer.history().get(0).topic());
    assertEquals("test_doc_3", producer.history().get(0).key());
    assertEquals(0, next.processDeleteCount);

    // Delete by query
    cmd = new DeleteUpdateCommand(req);
    cmd.query = "msg_text:foo";
    updateProcessor.processDelete(cmd);
    assertEquals(2 + expectedDocs, producer.history().size());
    for (int i = expectedDocs; i < producer.history().size(); i++) {
      ProducerRecord<String, byte[]> record = producer.history().get(i);
      assertEquals("c1", record.topic());
      assertEquals("msg_text:foo", record.key());
    }
    expectedDocs += 2;
    Set<Integer> partitions = new HashSet<>();
    partitions.add(producer.history().get(1).partition());
    partitions.add(producer.history().get(2).partition());
    assertEquals(new HashSet<>(Arrays.asList(0, 1)), partitions);
    assertEquals(0, next.processDeleteCount);

    // Test the behavior when no "next" is set, and no collection is set (does nothing)
    updateProcessor =
        new TestKafkaProducerUpdateProcessor(producer, req, new SolrQueryResponse(), null, null);
    updateProcessor.processDelete(cmd);

    // Test the behavior when "next" is set, and no collection is set
    updateProcessor =
        new TestKafkaProducerUpdateProcessor(producer, req, new SolrQueryResponse(), next, null);
    updateProcessor.processDelete(cmd);
    assertEquals(0, next.processAddCount);
    assertEquals(1, next.processDeleteCount);
  }
}
