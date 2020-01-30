package com.github.solrqueue.solrkafka;

import java.util.Collections;
import java.util.Map;
import org.apache.solr.common.util.NamedList;
import org.junit.Assert;
import org.junit.Test;

public class KafkaUpdateProcessorFactoryTest extends SingleCoreTestBase {
  @Test
  public void testInit() throws Exception {
    KafkaUpdateProcessorFactory factory = new KafkaUpdateProcessorFactory();

    Map[] badParams = {
      Collections.emptyMap(),
      Collections.singletonMap("bootstrap.servers", "bar"),
      Collections.singletonMap("field", "foo")
    };
    for (Map params : badParams) {
      try {
        factory.init(new NamedList<Object>(params));
        Assert.fail("Expected init exception without required params");
      } catch (IllegalArgumentException iae) { // expected
      }
    }
    NamedList<Object> params = new NamedList<>();
    /* Note: creating the producer doesn't immediately connect,
     * so localhost:9000 doesn't have to exist */
    params.add("bootstrap.servers", "localhost:9000");
    params.add("field", "_offset_");
    factory.init(params);
  }
}
