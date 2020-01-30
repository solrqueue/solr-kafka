package com.github.solrqueue.solrkafka;

import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.AddUpdateCommand;

/** Serializable representation of AddUpdateCommand */
public class KafkaAddUpdateCommand extends KafkaSerializableUpdateCommand<AddUpdateCommand> {

  /**
   * Wrap an AddUpdateCommand for serialization
   *
   * @param cmd the solr AddUpdateCommand
   */
  public KafkaAddUpdateCommand(AddUpdateCommand cmd) {
    super(cmd);
  }

  /**
   * Wrap a Record for deserialization
   *
   * @param core core context for deserialization
   * @param record record to deserialize
   * @throws IOException if error in deserialization
   */
  public KafkaAddUpdateCommand(SolrCore core, ConsumerRecord<String, byte[]> record)
      throws IOException {
    super(core, record);
  }

  @Override
  public String key(AddUpdateCommand cmd) {
    return cmd.getHashableId();
  }

  @Override
  public Object value(AddUpdateCommand cmd) {
    SimpleOrderedMap<Object> map = new SimpleOrderedMap<>();
    map.add("solrDoc", cmd.solrDoc);
    map.add("overwrite", cmd.overwrite);
    map.add("commitWithin", cmd.commitWithin);
    map.add("prevVersion", cmd.prevVersion);
    return map;
  }

  @Override
  protected void update(AddUpdateCommand cmd, Object value) {
    SimpleOrderedMap map = (SimpleOrderedMap) value;
    cmd.solrDoc = (SolrInputDocument) map.get("solrDoc");
    cmd.overwrite = (Boolean) map.get("overwrite");
    cmd.commitWithin = (Integer) map.get("commitWithin");
    cmd.prevVersion = (Long) map.get("prevVersion");
  }

  @Override
  protected AddUpdateCommand create(SolrQueryRequest req) {
    return new AddUpdateCommand(req);
  }
}
