package com.github.solrqueue.solrkafka;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Consumer loop, started by {@link KafkaUpdateProcessorFactory#inform(SolrCore)} */
public class KafkaUpdateConsumer implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static Map<CoreContainer, KafkaUpdateConsumer> instances = new HashMap<>();

  private CoreContainer coreContainer;
  private String bootstrapServers;
  private Consumer<String, byte[]> consumer;
  private Set<TopicPartition> assignments = Collections.emptySet();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicBoolean needToReassign = new AtomicBoolean(true);
  private String offsetField;
  private Long lastAssignmentCheck = 0L;
  protected int noTopicsPollMillis = 1000;
  Map<TopicPartition, KafkaConsumerUpdateProcessor> partitionToProcessor = new HashMap<>();

  protected KafkaUpdateConsumer(CoreContainer cc, String bootstrapServers, String offsetField) {
    this.offsetField = offsetField;
    this.coreContainer = cc;
    this.bootstrapServers = bootstrapServers;
    connect();
    startThread();
  }

  // Can override for testing
  protected void startThread() {
    Thread t = new Thread(this);
    t.setDaemon(true);
    t.start();
  }

  // Can override for testing
  protected CloudDescriptor getCloudDescriptor(SolrCore core) {
    return core.getCoreDescriptor().getCloudDescriptor();
  }

  /**
   * Main consumer thread. Checks for reassignments, polls for /update records and dispatches them
   * to the KafkaConsumerUpdateProcessor
   */
  public void run() {
    log.info("Started KafkaUpdateConsumer {}", coreContainer.getSolrHome());
    try {
      while (!closed.get()) {
        consumeAndReassign();
      }
    } catch (WakeupException | InterruptedException e) {
      // Ignore exception if closing
      if (!closed.get()) {
        throw new RuntimeException(e);
      }
    } catch (Exception e) {
      if (!closed.get()) {
        log.error("unexpected exit", e);
      }
    } finally {
      log.info("Closing consumer {}", coreContainer.getSolrHome());
      consumer.close(Duration.ofSeconds(1));
      log.info("Consumer closed {} ", coreContainer.getSolrHome());
    }
  }

  void consumeAndReassign() throws Exception {
    if (coreContainer.isShutDown()) {
      closed.set(true);
      return;
    }
    Long now = System.currentTimeMillis();
    if (needToReassign.compareAndSet(true, false)
        || assignments.isEmpty()
        || now - lastAssignmentCheck > 10000) {
      doReassign();
    }
    if (assignments.isEmpty()) {
      Thread.sleep(noTopicsPollMillis);
      return;
    }
    log.debug("poll start");
    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(10000));
    log.debug("poll exit");

    for (TopicPartition partition : records.partitions()) {
      KafkaConsumerUpdateProcessor processor = partitionToProcessor.get(partition);
      List<ConsumerRecord<String, byte[]>> partitionRecords = records.records(partition);

      try {
        for (ConsumerRecord<String, byte[]> record : partitionRecords) {
          processor.process(record);
        }
      } catch (IOException ioe) {
        log.error("Failed to process records for {}", partition, ioe);
      }
    }
  }

  // Shutdown hook which can be called from a separate thread
  public void shutdown() {
    closed.set(true);
    consumer.wakeup();
  }

  /**
   * Using the offsetField, find the latest committed offset in the index getMaxVersionFromIndex
   * borrows from VersionInfo.getMaxVersionFromIndex, and assumes that offsetField is docValued
   *
   * @param core Core to fetch index from
   * @return Offset, or null if none found
   */
  Long loadOffset(SolrCore core) {
    IndexSchema schema = core.getLatestSchema();
    SchemaField schemaField = schema.getFieldOrNull(offsetField);
    if (schemaField == null) {
      throw new RuntimeException(
          String.format(
              "Cannot load offset for core %s because offset field %s not in schema",
              core.getName(), offsetField));
    }

    if (!schemaField.hasDocValues()) {
      throw new RuntimeException(
          String.format(
              "Cannot load offset for core %s because offset field %s not docValued",
              core.getName(), offsetField));
    }

    long maxOffsetInIndex = Long.MIN_VALUE;
    boolean maxOffsetIsSet = false;
    ValueSource vs = schemaField.getType().getValueSource(schemaField, null);
    RefCounted<SolrIndexSearcher> searcherRef = null;
    try {
      searcherRef = core.getSearcher();
      SolrIndexSearcher searcher = searcherRef.get();
      Map funcContext = ValueSource.newContext(searcher);
      vs.createWeight(funcContext, searcher);
      // TODO: multi-thread this
      for (LeafReaderContext ctx : searcher.getTopReaderContext().leaves()) {
        int maxDoc = ctx.reader().maxDoc();
        FunctionValues fv = vs.getValues(funcContext, ctx);
        for (int doc = 0; doc < maxDoc; doc++) {
          long v = fv.longVal(doc);
          maxOffsetInIndex = Math.max(v, maxOffsetInIndex);
          maxOffsetIsSet = true;
        }
      }
      if (maxOffsetIsSet) {
        log.info("Loaded offset for {},  {}:{} + 1", core.getName(), offsetField, maxOffsetInIndex);
        return maxOffsetInIndex;
      } else {
        log.info("No offsets for {} (empty core?)", core.getName());
      }

    } catch (IOException ioe) {
      log.error("Failed to read offset from {}", core.getName(), ioe);
    } finally {
      if (searcherRef != null) {
        searcherRef.decref();
      }
    }
    return null;
  }

  private void connect() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Need to disable this so that we can have more than one replica
    // pulling from the stream.  we'll derive the offsets from the index
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumer = KafkaClientFactory.INSTANCE.consumer(props);
  }

  /**
   * Map all cores in the CoreContainer to TopicPartitions in Kafka. Create
   * KafkaConsumerUpdateProcessor instances for any new/changed cores
   */
  private void doReassign() {
    log.info("reassigning consumer requested");
    Set<TopicPartition> reassignments = new HashSet<>();
    Map<TopicPartition, KafkaConsumerUpdateProcessor> reassignedPartitionToProcessor =
        new HashMap<>();

    for (SolrCore core : coreContainer.getCores()) {
      CloudDescriptor cd = getCloudDescriptor(core);
      if (cd == null) {
        log.info("No cloud descriptor found for core {}", core.getName());
        continue;
      }
      log.info("Checking {} {} {}", core.getName(), cd.getCollectionName(), cd.getShardId());
      Integer partition = KafkaProducerUpdateProcessor.slicePartition(cd.getShardId());
      if (partition == null) continue;
      TopicPartition tp = new TopicPartition(cd.getCollectionName(), partition);
      log.info("mapping {} to {}", core.getName(), tp);
      reassignments.add(tp);
      KafkaConsumerUpdateProcessor currentProcessor = partitionToProcessor.get(tp);

      if (currentProcessor == null || currentProcessor.getCore() != core) {
        reassignedPartitionToProcessor.put(tp, new KafkaConsumerUpdateProcessor(core, offsetField));
      } else {
        reassignedPartitionToProcessor.put(tp, currentProcessor);
      }
    }

    if (!reassignments.equals(assignments)) {
      log.info("Found {} assignments", reassignments.size());
      consumer.assign(reassignments);
    }

    for (TopicPartition assignment : reassignments) {
      if (!assignments.contains(assignment)) {
        KafkaConsumerUpdateProcessor processor = reassignedPartitionToProcessor.get(assignment);
        Long offset = loadOffset(processor.getCore());
        if (offset != null) {
          /* Calling consumer.assign() starts an async job to fetch positions
           * which races with our consumer.seek().  fetching and discarding
           * the position should fix this by forcing the metadata fetch to complete
           */
          consumer.position(assignment);
          log.info("new assignment {} seek to {}", assignment, offset + 1);
          consumer.seek(assignment, offset + 1);
          processor.setCurrentOffset(offset);
        }
      }
    }
    assignments = reassignments;
    partitionToProcessor = reassignedPartitionToProcessor;
    lastAssignmentCheck = System.currentTimeMillis();
  }

  /** Request a core to topic reassignment ASAP */
  public void reassign() {
    needToReassign.set(true);
  }

  /**
   * Get the singleton instance that consumes for all topics on a node
   *
   * @param cc CoreContainer for node
   * @param bootstrapServers comma-separated list of kafka brokers to connect to, see
   *     bootstrap.servers
   * @param offsetField name of SolrField to store kafka offset on. Should be docValued field
   * @return singleton KafkaUpdateConsumer instance
   */
  public static synchronized KafkaUpdateConsumer getInstance(
      CoreContainer cc, String bootstrapServers, String offsetField) {

    // In tests, it is possible to have more than one CoreContainer per jvm
    KafkaUpdateConsumer instance =
        instances.computeIfAbsent(
            cc, (cc1) -> new KafkaUpdateConsumer(cc1, bootstrapServers, offsetField));
    return instance;
  }
}
