package com.github.solrqueue.solrkafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.solr.common.util.DataInputInputStream;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.UpdateCommand;

/**
 * Serializable representations of UpdateCommand. Subclasses must implement String key(), Object
 * value()
 */
public abstract class KafkaSerializableUpdateCommand<T extends UpdateCommand> {
  protected T cmd;

  protected KafkaSerializableUpdateCommand(T cmd) {
    this.cmd = cmd;
  }

  protected KafkaSerializableUpdateCommand(SolrCore core, ConsumerRecord<String, byte[]> record)
      throws IOException {
    this.cmd = command(core, record);
  }

  public T getCommand() {
    return cmd;
  }

  /** Helper class for creating UTF-8 serialized Record headers */
  public static class BasicHeader implements Header {
    private final String headerKey;
    private final byte[] headerValue;

    public BasicHeader(String key, String value) {
      this.headerKey = key;

      try {
        if (value != null) {
          headerValue = value.getBytes("UTF-8");
        } else {
          headerValue = null;
        }
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("value", uee);
      }
    }

    @Override
    public String key() {
      return this.headerKey;
    }

    @Override
    public byte[] value() {
      return headerValue;
    }
  }

  /**
   * Object representing UpdateCommand subclass fields
   *
   * @param cmd instance of UpdateCommand subclass
   * @return Object to serialize using JavaBinCodec
   */
  protected abstract Object value(T cmd);

  /**
   * String to be used as key for Record
   *
   * @param cmd instance of UpdateCommand subclass
   * @return record key
   */
  protected abstract String key(T cmd);

  /**
   * UpdateCommand constructor
   *
   * @return instance of UpdateCommand subclass
   */
  protected abstract T create(SolrQueryRequest req);

  /**
   * Updates command with value data on deserialization
   *
   * @param cmd instance of UpdateCommand subclass
   * @param value deserialized value object (see value(T))
   */
  protected abstract void update(T cmd, Object value);

  private static byte[] serialize(Object doc) throws IOException {
    JavaBinCodec codec = new JavaBinCodec();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FastOutputStream fos = FastOutputStream.wrap(baos);
    codec.init(fos);
    codec.writeVal(doc);
    fos.flush();
    return baos.toByteArray();
  }

  protected static SimpleOrderedMap deserialize(byte[] data) throws IOException {
    JavaBinCodec codec = new JavaBinCodec();
    DataInputInputStream dis = new FastInputStream(new ByteArrayInputStream(data));
    SimpleOrderedMap map = (SimpleOrderedMap) codec.readVal(dis);
    return map;
  }

  protected T command(SolrCore core, ConsumerRecord<String, byte[]> record) throws IOException {
    SimpleOrderedMap map = deserialize(record.value());
    LocalSolrQueryRequest req = new LocalSolrQueryRequest(core, (NamedList) map.get("req.params"));
    T cmd = create(req);

    Integer flags = (Integer) map.get("flags");
    if (flags != null) {
      cmd.setFlags(flags);
    }

    cmd.setRoute((String) map.get("route"));

    Long version = (Long) map.get("version");
    if (version != null) {
      cmd.setVersion(version);
    }

    Object value = map.get("value");
    if (value != null) {
      update(cmd, value);
    }

    return cmd;
  }

  /**
   * Builds a Record from the UpdateCommand. Constructs and serializes a JavaBinCodec payload
   *
   * @param topic Record topic
   * @param partition Record partition
   * @return Record representing the UpdateCommand
   * @throws IOException if there is an issue serializing the value
   */
  public ProducerRecord<String, byte[]> record(String topic, Integer partition) throws IOException {
    SimpleOrderedMap<Object> object = new SimpleOrderedMap<>();
    // Add basic UpdateCommand properties
    object.add("name", cmd.name());
    object.add("flags", cmd.getFlags());
    object.add("route", cmd.getRoute());
    object.add("version", cmd.getVersion());

    object.add("req.params", cmd.getReq().getParams().toNamedList());

    Object cmdValue = value(cmd);
    if (cmdValue != null) {
      object.add("value", cmdValue);
    }
    byte[] valueBytes = serialize(object);
    List<Header> headers =
        Collections.singletonList(new BasicHeader("class", getClass().getName()));
    return new ProducerRecord<>(topic, partition, null, key(cmd), valueBytes, headers);
  }

  /**
   * Get the KafkaSerializableUpdateCommand subclass name from the Record header list
   *
   * @param headers Record headers
   * @return subclass class name, or null it not found
   */
  public static String extractClassName(Headers headers) {
    Header h = headers.lastHeader("class");
    if (h == null) {
      return null;
    }
    if (h.value() == null) {
      return null;
    }
    try {
      return new String(h.value(), "UTF-8");
    } catch (UnsupportedEncodingException uee) {
      return null;
    }
  }
}
