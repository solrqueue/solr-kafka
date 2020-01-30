package com.github.solrqueue.solrkafka;

import java.util.Collections;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

class TestKafkaConsumerUpdateProcessor extends KafkaConsumerUpdateProcessor {
  UpdateRequestProcessorFactory next;

  TestKafkaConsumerUpdateProcessor(
      SolrCore core, String offsetField, UpdateRequestProcessorFactory next) {
    super(core, offsetField);
    this.next = next;
  }

  @Override
  UpdateRequestProcessorChain restOfChain(UpdateRequestProcessorChain chain) {
    return new UpdateRequestProcessorChain(Collections.singletonList(next), core);
  }
}
