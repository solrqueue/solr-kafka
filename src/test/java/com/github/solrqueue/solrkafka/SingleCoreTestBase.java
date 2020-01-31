package com.github.solrqueue.solrkafka;

import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.junit.BeforeClass;

public class SingleCoreTestBase extends SolrTestCaseJ4 {

  protected LocalSolrQueryRequest emptyReq() {
    return new LocalSolrQueryRequest(getTestCore(), Collections.emptyMap());
  }

  protected <K, V> ConsumerRecord<K, V> consumerize(ProducerRecord<K, V> r, long offset) {
    return new ConsumerRecord<K, V>(
        r.topic(),
        r.partition(),
        offset,
        -1L,
        TimestampType.NO_TIMESTAMP_TYPE,
        -1L,
        -1,
        -1,
        r.key(),
        r.value(),
        r.headers());
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    initCore("solrconfig.xml", "managed-schema", "src/test/resources/solr_home", "testcore");
  }

  public SolrCore getTestCore() {
    return h.getCore();
  }
}
