package com.github.solrqueue.solrkafka;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.update.AddUpdateCommand;
import org.junit.Test;

public class KafkaAddUpdateCommandTest extends SingleCoreTestBase {

  @Test
  public void testSerialization() throws Exception {
    String id = "test123";
    AddUpdateCommand cmd = new AddUpdateCommand(emptyReq());
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", id);
    doc.addField("fooField", Collections.singletonMap("set", "barValue"));
    cmd.solrDoc = doc;
    cmd.overwrite = true;
    cmd.commitWithin = 123;
    KafkaAddUpdateCommand kafkaCmd = new KafkaAddUpdateCommand(cmd);
    ProducerRecord<String, byte[]> record = kafkaCmd.record("testTopic", 1);
    assertEquals(id, record.key());
    AddUpdateCommand deserialized =
        new KafkaAddUpdateCommand(getTestCore(), consumerize(record, 0)).getCommand();
    assertEquals(123, deserialized.commitWithin);
    assertEquals(true, deserialized.overwrite);
    assertEquals(id, deserialized.getHashableId());
    assertEquals("barValue", ((Map) deserialized.solrDoc.getFieldValue("fooField")).get("set"));
  }
}
