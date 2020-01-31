package com.github.solrqueue.solrkafka;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.DistributedUpdateProcessorFactory;
import org.apache.solr.update.processor.RunUpdateProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.junit.Assert;
import org.junit.Test;

public class KafkaConsumerUpdateProcessorTest extends SingleCoreTestBase {
  @Test
  public void testProcessAdd() throws Exception {
    long offset = 42;
    TestUpdateRequestProcessor.Factory f = new TestUpdateRequestProcessor.Factory();
    TestKafkaConsumerUpdateProcessor c =
        new TestKafkaConsumerUpdateProcessor(getTestCore(), "_offset_", f);

    /////////
    // ADD //
    /////////
    String id = "test_id_1";
    AddUpdateCommand cmd = new AddUpdateCommand(emptyReq());
    cmd.solrDoc = new SolrInputDocument();
    cmd.solrDoc.setField("id", id);
    ProducerRecord<String, byte[]> r = new KafkaAddUpdateCommand(cmd).record("c1", 0);
    c.process(consumerize(r, offset));
    assertEquals(id, f.lastInstance.lastAdd.solrDoc.getFieldValue("id"));
    assertEquals(offset, f.lastInstance.lastAdd.solrDoc.getFieldValue("_offset_"));

    // Update format, no existing doc
    cmd.solrDoc = new SolrInputDocument();
    cmd.solrDoc.setField("id", id);
    cmd.solrDoc.setField("msg_txt", Collections.singletonMap("set", "hi"));
    r = new KafkaAddUpdateCommand(cmd).record("c1", 0);
    c.process(consumerize(r, offset));
    assertEquals("hi", f.lastInstance.lastAdd.solrDoc.getFieldValue("msg_txt"));

    // Update format, existing doc
    AddUpdateCommand directCmd = new AddUpdateCommand(emptyReq());
    directCmd.solrDoc = new SolrInputDocument();
    directCmd.solrDoc.setField("id", "real_doc_1");
    directCmd.solrDoc.setField("msg1_txt", "hi_1");
    getTestCore().getUpdateHandler().addDoc(directCmd);

    AddUpdateCommand updateCmd = new AddUpdateCommand(emptyReq());
    updateCmd.solrDoc = new SolrInputDocument();
    updateCmd.solrDoc.setField("id", "real_doc_1");
    updateCmd.solrDoc.setField("msg2_txt", Collections.singletonMap("set", "hi_2"));
    r = new KafkaAddUpdateCommand(updateCmd).record("c1", 0);
    c.process(consumerize(r, offset));
    // confirm existing doc was brought in
    assertEquals("hi_1", f.lastInstance.lastAdd.solrDoc.getFieldValue("msg1_txt"));
    assertEquals("hi_2", f.lastInstance.lastAdd.solrDoc.getFieldValue("msg2_txt"));

    // Try setting an explicit version on the document
    updateCmd.solrDoc.setField("_version_", 1337L);
    r = new KafkaAddUpdateCommand(updateCmd).record("c1", 0);
    c.process(consumerize(r, offset));

    ////////////
    // DELETE //
    ////////////

    DeleteUpdateCommand deleteCmd = new DeleteUpdateCommand(emptyReq());
    deleteCmd.id = "test_doc_1";
    r = new KafkaDeleteUpdateCommand(deleteCmd).record("c1", 0);
    c.process(consumerize(r, offset));
    assertEquals("test_doc_1", f.lastInstance.lastDelete.id);
  }

  @Test
  public void testRestOfChain() throws Exception {
    KafkaConsumerUpdateProcessor c = new KafkaConsumerUpdateProcessor(getTestCore(), "_offset_");

    // Test bad chains (kafka not in processors, in wrong order, etc)
    List<List<UpdateRequestProcessorFactory>> badChains = new ArrayList<>();
    badChains.add(Collections.emptyList());
    badChains.add(Collections.singletonList(new DistributedUpdateProcessorFactory()));
    for (List<UpdateRequestProcessorFactory> chain : badChains) {
      try {
        UpdateRequestProcessorChain noKafka = new UpdateRequestProcessorChain(chain, getTestCore());
        c.restOfChain(noKafka);
        Assert.fail("invalid chain");
      } catch (Exception e) { // expected
      }
    }

    // Test a good chain
    RunUpdateProcessorFactory r = new RunUpdateProcessorFactory();
    List<UpdateRequestProcessorFactory> goodChain = new ArrayList<>();
    goodChain.add(new KafkaUpdateProcessorFactory());
    goodChain.add(new DistributedUpdateProcessorFactory());
    goodChain.add(r);
    UpdateRequestProcessorChain rest =
        c.restOfChain(new UpdateRequestProcessorChain(goodChain, getTestCore()));
    assertEquals(Collections.singletonList(r), rest.getProcessors());
  }
}
