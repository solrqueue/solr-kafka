package com.github.solrqueue.solrkafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessor;

/**
 * UpdateRequestProcessor that sends updates over Kafka Similar to DistributedUpdateRequestProcessor
 * except:
 *
 * <ul>
 *   <li>Update is sent to a kafka topic + partition representing the shard instead of to specific
 *       replica nodes over HTTP
 *   <li>Update is <em>not</em> applied locally immediately. The consumer side will poll kafka and
 *       apply the update to this node
 * </ul>
 */
public class KafkaProducerUpdateProcessor extends UpdateRequestProcessor {

  private final Producer<String, byte[]> producer;
  private final SolrQueryRequest req;
  private final SolrQueryResponse rsp;
  private final boolean skipDefault;
  private final ZkController zkController;
  private final List<Future<RecordMetadata>> futures = new ArrayList<>();
  protected String collection;

  /**
   * Create a KafkaProducerUpdateProcessor instance, called by {@link
   * KafkaUpdateProcessorFactory#getInstance}
   *
   * @param producer KafkaProducer client
   * @param req Solr request to process
   * @param rsp Solr response object
   * @param next Next processor in the chain
   * @param skipDefault default behavior for whether this processor should be skipped (fall through
   *     to next)
   */
  public KafkaProducerUpdateProcessor(
      Producer<String, byte[]> producer,
      SolrQueryRequest req,
      SolrQueryResponse rsp,
      UpdateRequestProcessor next,
      boolean skipDefault) {
    super(next);
    this.producer = producer;
    this.rsp = rsp;
    this.req = req;

    this.skipDefault = skipDefault;
    CoreContainer cc = req.getCore().getCoreContainer();
    CloudDescriptor cloudDesc = req.getCore().getCoreDescriptor().getCloudDescriptor();

    zkController = cc.getZkController();

    if (cloudDesc != null) {
      this.collection = cloudDesc.getCollectionName();
    } else {
      this.collection = null;
    }
  }

  /**
   * Gets the zkController's clusterstate. Overridden in tests
   *
   * @return cluster's state information
   */
  protected ClusterState getClusterState() {
    return zkController.getClusterState();
  }

  private List<TopicPartition> requestPartitions(String id, SolrInputDocument doc) {
    return requestPartitions(id, doc, null);
  }

  /**
   * Determine if submitting to kafka should be skipped. If so, the next processor in the chain
   * should be invoked
   *
   * @param cmd the update/delete command
   * @return true if should skip submitting to kafka
   */
  private boolean shouldSkip(UpdateCommand cmd) {
    SolrParams params = cmd.getReq().getParams();

    /* Parameters like kafka.skip do not always get passed down to
     * the replicas if skipped, it depends on which update handler
     * was used.  If we are on the receiving end of a distributed update,
     * from a leader, then kafka was skipped at the leader, so no point
     * in trying to submit via kafka in that case, since the intention was to skip.
     */
    if (params.get(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM) != null) {
      return true;
    }
    return collection == null
        || params.getBool(KafkaUpdateProcessorFactory.KAFKA_SKIP_PARAM, skipDefault);
  }

  /**
   * Derive the Kafka partition number from the Solr shard name/id. E.g. shard1 -> 0, shard2 -> 1
   *
   * @param shardId name of the Solr shard
   * @return partition number, or null if shard name is not in expected format
   */
  static Integer slicePartition(String shardId) {
    if (shardId.startsWith("shard")) {
      try {
        int shardIdNum = Integer.parseInt(shardId.substring(5));
        if (shardIdNum < 1) {
          return null;
        }
        // Kafka partition ids are 0-indexed
        return shardIdNum - 1;
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private List<TopicPartition> requestPartitions(String id, SolrInputDocument doc, String route) {
    ClusterState cstate = getClusterState();
    DocCollection coll = cstate.getCollection(collection);
    Slice slice = coll.getRouter().getTargetSlice(id, doc, route, req.getParams(), coll);
    Integer partitionId = slicePartition(slice.getName());
    if (partitionId != null) {
      return Collections.singletonList(new TopicPartition(collection, partitionId));
    }
    return null;
  }

  private List<TopicPartition> doDeleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    DocCollection coll = getClusterState().getCollection(collection);
    SolrParams params = req.getParams();
    String route = params.get(ShardParams._ROUTE_);
    Collection<Slice> slices = coll.getRouter().getSearchSlices(route, params, coll);
    List<TopicPartition> partitions = new ArrayList<>(slices.size());
    for (Slice slice : slices) {
      Integer partitionId = slicePartition(slice.getName());
      if (partitionId == null) {
        return null;
      }
      partitions.add(new TopicPartition(collection, partitionId));
    }
    return partitions;
  }

  /**
   * Send AddUpdateCommand to Kafka
   *
   * @param cmd the command to process
   * @throws IOException if an error is encountered while sending
   */
  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    if (shouldSkip(cmd)) {
      if (next != null) {
        next.processAdd(cmd);
      }
      return;
    }
    String id = cmd.getHashableId();
    SolrInputDocument doc = cmd.getSolrInputDocument();
    List<TopicPartition> partitions = requestPartitions(id, doc);
    KafkaAddUpdateCommand kafkaCmd = new KafkaAddUpdateCommand(cmd);
    if (partitions != null && !partitions.isEmpty()) {
      emitRecords(kafkaCmd, partitions);
    } else if (next != null) {
      next.processAdd(cmd);
    }
  }

  /**
   * Send DeleteUpdateCommand to Kafka. For delete by id, routed to the appropriate shard. For
   * delete by query, sent to all shard, or if a route is present, to the subset in the route
   *
   * @param cmd the command to process
   * @throws IOException if an error is encountered while sending
   */
  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    if (shouldSkip(cmd)) {
      if (next != null) {
        next.processDelete(cmd);
      }
      return;
    }
    List<TopicPartition> partitions;
    if (!cmd.isDeleteById()) {
      partitions = doDeleteByQuery(cmd);
    } else {
      partitions = requestPartitions(cmd.getId(), null, cmd.getRoute());
    }
    if (partitions != null && !partitions.isEmpty()) {
      emitRecords(new KafkaDeleteUpdateCommand(cmd), partitions);
    } else if (next != null) {
      next.processDelete(cmd);
    }
  }

  private void emitRecords(KafkaSerializableUpdateCommand<?> cmd, List<TopicPartition> partitions)
      throws IOException {
    for (TopicPartition tp : partitions) {
      ProducerRecord<String, byte[]> r = cmd.record(tp.topic(), tp.partition());
      futures.add(producer.send(r));
    }
  }

  /**
   * Add response data. Resolve the Kafka futures and process any offset/error info
   *
   * @throws IOException (not used, exceptions are converted to SolrExceptions)
   */
  @Override
  public void finish() throws IOException {
    if (!futures.isEmpty()) {
      SolrException solrException = null;
      NamedList<String> exceptionMetadata = new NamedList<>();
      List<SimpleOrderedMap<Object>> outList = new ArrayList<>();
      for (Future<RecordMetadata> f : futures) {
        try {
          RecordMetadata r = f.get();
          SimpleOrderedMap<Object> out = new SimpleOrderedMap<>();
          out.add("topic", r.topic());
          out.add("partition", r.partition());
          if (r.hasOffset()) {
            out.add("offset", r.offset());
          }
          outList.add(out);
        } catch (Exception e) {
          solrException = new SolrException(ErrorCode.SERVER_ERROR, e.getMessage(), e);
          exceptionMetadata.add("message", e.toString());
        }
      }
      if (solrException != null) {
        solrException.setMetadata(exceptionMetadata);
        throw solrException;
      }
      rsp.add("kafka", outList);
    }
    if (next != null && futures.isEmpty()) next.finish();
  }
}
