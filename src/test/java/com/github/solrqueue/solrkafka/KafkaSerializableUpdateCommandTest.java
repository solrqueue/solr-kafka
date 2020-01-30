package com.github.solrqueue.solrkafka;

import static org.junit.Assert.assertEquals;

import com.github.solrqueue.solrkafka.KafkaSerializableUpdateCommand.BasicHeader;
import java.util.Collections;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

public class KafkaSerializableUpdateCommandTest extends SingleCoreTestBase {
  @Test
  public void testExtractClassName() {
    // Valid header
    ProducerRecord<String, String> r =
        new ProducerRecord<>(
            "topic",
            1,
            "foo",
            "bar",
            Collections.singletonList(new BasicHeader("class", "TestClassName")));
    assertEquals("TestClassName", KafkaSerializableUpdateCommand.extractClassName(r.headers()));

    // No headers
    r = new ProducerRecord<>("topic", 1, "foo", "bar");
    assertEquals(null, KafkaSerializableUpdateCommand.extractClassName(r.headers()));

    // Null header
    r =
        new ProducerRecord<>(
            "topic", 1, "foo", "bar", Collections.singletonList(new BasicHeader("class", null)));
    assertEquals(null, KafkaSerializableUpdateCommand.extractClassName(r.headers()));
  }

  @Test
  public void testBasicHeader() throws Exception {
    String key = "key";
    String value = "value123";

    BasicHeader bh = new BasicHeader(key, value);
    assertEquals(key, bh.key());
    Assert.assertArrayEquals(value.getBytes("UTF-8"), bh.value());
  }
}
