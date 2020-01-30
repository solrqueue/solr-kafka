package com.github.solrqueue.solrkafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.junit.Assert;
import org.junit.Test;

public class KafkaUpdateConsumerTest extends SingleCoreTestBase {
  static int testRetryPollMillis = 100;

  static class TestKafkaUpdateConsumer extends KafkaUpdateConsumer {
    Map<SolrCore, CloudDescriptor> cloudDescMap;

    TestKafkaUpdateConsumer(CoreContainer cc, String bootstrapServers, String offsetField) {
      super(cc, bootstrapServers, offsetField);
      noTopicsPollMillis = testRetryPollMillis;
    }

    protected void startThread() {
      // Don't start a thread in unit tests
    }

    protected CloudDescriptor getCloudDescriptor(SolrCore core) {
      return (cloudDescMap == null) ? null : cloudDescMap.get(core);
    }
  }

  @Test
  public void testRunShutdown() {
    MockConsumer<String, byte[]> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    KafkaClientFactory.INSTANCE = new TestKafkaClientFactory(kafkaConsumer);
    TestKafkaUpdateConsumer c =
        new TestKafkaUpdateConsumer(testCore.getCoreContainer(), "localhost:9000", "_offset_");
    c.reassign();
    c.shutdown();
    c.run();
    // If we don't exit, shutdown didn't work
  }

  @Test
  public void testConsumeAndReassign() throws Exception {
    MockConsumer<String, byte[]> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    TopicPartition tp = new TopicPartition("c1", 0);
    kafkaConsumer.updateEndOffsets(Collections.singletonMap(tp, 0L));
    KafkaClientFactory.INSTANCE = new TestKafkaClientFactory(kafkaConsumer);
    TestKafkaUpdateConsumer c =
        new TestKafkaUpdateConsumer(testCore.getCoreContainer(), "localhost:9000", "_offset_");

    long start = System.currentTimeMillis();
    // Confirm that if assignments are empty, we just return after a pause
    c.consumeAndReassign();
    boolean inRange = System.currentTimeMillis() - start >= testRetryPollMillis - 10;
    assertTrue("when no assignment, waited before retry", inRange);

    Properties cloudProps = new Properties();
    cloudProps.put(CoreDescriptor.CORE_SHARD, "shard1");
    cloudProps.put(CoreDescriptor.CORE_COLLECTION, "c1");
    c.cloudDescMap =
        Collections.singletonMap(
            testCore,
            new CloudDescriptor(testCore.getName(), cloudProps, testCore.getCoreDescriptor()));
    c.consumeAndReassign();

    // Confirm that this core, (c1, shard1) was assigned to TopicPartition (c1, 0)
    KafkaConsumerUpdateProcessor p = c.partitionToProcessor.get(tp);
    assertEquals(testCore, p.getCore());

    TestUpdateRequestProcessor.Factory f = new TestUpdateRequestProcessor.Factory();
    c.partitionToProcessor.put(tp, new TestKafkaConsumerUpdateProcessor(testCore, "_offset_", f));

    AddUpdateCommand cmd = new AddUpdateCommand(emptyReq());
    cmd.solrDoc = new SolrInputDocument();
    cmd.solrDoc.setField("id", "hi");
    KafkaAddUpdateCommand kafkaCmd = new KafkaAddUpdateCommand(cmd);
    ConsumerRecord<String, byte[]> cr = consumerize(kafkaCmd.record(tp.topic(), tp.partition()), 0);
    kafkaConsumer.addRecord(cr);

    c.consumeAndReassign();
    assertEquals("hi", f.lastInstance.lastAdd.solrDoc.getFieldValue("id"));
  }

  @Test
  public void testConsumeAndReassignFromOffset() throws Exception {
    MockConsumer<String, byte[]> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    TopicPartition tp = new TopicPartition("c1", 0);
    kafkaConsumer.updateEndOffsets(Collections.singletonMap(tp, 0L));
    KafkaClientFactory.INSTANCE = new TestKafkaClientFactory(kafkaConsumer);
    TestKafkaUpdateConsumer c =
        new TestKafkaUpdateConsumer(testCore.getCoreContainer(), "localhost:9000", "_offset_");

    Properties cloudProps = new Properties();
    cloudProps.put(CoreDescriptor.CORE_SHARD, "shard1");
    cloudProps.put(CoreDescriptor.CORE_COLLECTION, "c1");
    c.cloudDescMap =
        Collections.singletonMap(
            testCore,
            new CloudDescriptor(testCore.getName(), cloudProps, testCore.getCoreDescriptor()));
    AddUpdateCommand directCmd = new AddUpdateCommand(emptyReq());
    directCmd.solrDoc = new SolrInputDocument();
    directCmd.solrDoc.setField("id", "real_doc_42");
    directCmd.solrDoc.setField("_offset_", 42);
    testCore.getUpdateHandler().addDoc(directCmd);
    testCore.getUpdateHandler().commit(new CommitUpdateCommand(emptyReq(), false));

    c.consumeAndReassign();
    assertEquals(43, kafkaConsumer.position(tp));
  }

  @Test
  public void testLoadOffset() throws Exception {
    TestKafkaUpdateConsumer c =
        new TestKafkaUpdateConsumer(testCore.getCoreContainer(), "localhost:9000", "_offset_");
    // Should be null offset if no docs yet
    Long offset = c.loadOffset(testCore);
    Assert.assertNull(offset);

    // add some docs to confirm offset calculation
    for (long docOffset : new long[] {1L, 3L, 2L}) {
      AddUpdateCommand directCmd = new AddUpdateCommand(emptyReq());
      directCmd.solrDoc = new SolrInputDocument();
      directCmd.solrDoc.setField("id", "real_doc_" + docOffset);
      directCmd.solrDoc.setField("_offset_", docOffset);
      testCore.getUpdateHandler().addDoc(directCmd);
    }
    testCore.getUpdateHandler().commit(new CommitUpdateCommand(emptyReq(), false));
    offset = c.loadOffset(testCore);
    assertEquals(Long.valueOf(3L), offset);

    // expect an exception if the offset field can't be resolved in schema
    try {
      new TestKafkaUpdateConsumer(testCore.getCoreContainer(), "localhost:9000", "_foobar_")
          .loadOffset(testCore);
      Assert.fail("expected an exception if offset field can't be resolved");
    } catch (Exception e) {
    }
  }
}
