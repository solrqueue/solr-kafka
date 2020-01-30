package com.github.solrqueue.solrkafka;

import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.DeleteUpdateCommand;

/** Serializable representation of DeleteUpdateCommand */
public class KafkaDeleteUpdateCommand extends KafkaSerializableUpdateCommand<DeleteUpdateCommand> {
  public static String UNSPECIFIED = "UNSPECIFIED_DELETE";

  /**
   * Wrap a DeleteUpdateCommand for serialization
   *
   * @param cmd the solr DeleteUpdateCommand
   */
  public KafkaDeleteUpdateCommand(DeleteUpdateCommand cmd) {
    super(cmd);
  }

  /**
   * Wrap a Record for deserialization
   *
   * @param core core context for deserialization
   * @param record record to deserialize
   * @throws IOException if error in deserialization
   */
  public KafkaDeleteUpdateCommand(SolrCore core, ConsumerRecord<String, byte[]> record)
      throws IOException {
    super(core, record);
  }

  @Override
  protected String key(DeleteUpdateCommand cmd) {
    if (cmd.id != null) {
      return cmd.id;
    } else if (cmd.query != null) {
      return cmd.query;
    }
    return UNSPECIFIED;
  }

  @Override
  protected Object value(DeleteUpdateCommand cmd) {
    SimpleOrderedMap<Object> map = new SimpleOrderedMap<>();
    map.add("id", cmd.id);
    map.add("query", cmd.query);
    map.add("commitWithin", cmd.commitWithin);
    return map;
  }

  @Override
  protected void update(DeleteUpdateCommand cmd, Object value) {
    SimpleOrderedMap map = (SimpleOrderedMap) value;
    cmd.id = (String) map.get("id");
    cmd.query = (String) map.get("query");
    cmd.commitWithin = (Integer) map.get("commitWithin");
  }

  @Override
  protected DeleteUpdateCommand create(SolrQueryRequest req) {
    return new DeleteUpdateCommand(req);
  }
}
