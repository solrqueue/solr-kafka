package com.github.solrqueue.solrkafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.solr.update.DeleteUpdateCommand;
import org.junit.Test;

public class KafkaDeleteUpdateCommandTest extends SingleCoreTestBase {

  @Test
  public void testSerialization() throws Exception {
    // Delete by id
    String id = "test123";
    DeleteUpdateCommand cmd = new DeleteUpdateCommand(emptyReq());
    cmd.id = id;
    cmd.commitWithin = 123;
    KafkaDeleteUpdateCommand kafkaCmd = new KafkaDeleteUpdateCommand(cmd);
    ProducerRecord<String, byte[]> record = kafkaCmd.record("testTopic", 1);
    assertEquals(id, record.key());
    DeleteUpdateCommand deserialized =
        new KafkaDeleteUpdateCommand(getTestCore(), consumerize(record, 0)).getCommand();
    assertEquals(123, deserialized.commitWithin);
    assertEquals(id, deserialized.id);
    assertNull(deserialized.query);

    // Delete by query
    String query = "msg_text:hi";
    cmd = new DeleteUpdateCommand(emptyReq());
    cmd.query = query;
    cmd.commitWithin = 123;
    kafkaCmd = new KafkaDeleteUpdateCommand(cmd);
    record = kafkaCmd.record("testTopic", 1);
    assertEquals(query, record.key());
    deserialized = new KafkaDeleteUpdateCommand(getTestCore(), consumerize(record, 0)).getCommand();
    assertEquals(123, deserialized.commitWithin);
    assertEquals(query, deserialized.query);
    assertNull(deserialized.id);

    // Empty delete, somehow...
    cmd = new DeleteUpdateCommand(emptyReq());
    kafkaCmd = new KafkaDeleteUpdateCommand(cmd);
    record = kafkaCmd.record("testTopic", 1);
    assertEquals(KafkaDeleteUpdateCommand.UNSPECIFIED, record.key());
  }
}
