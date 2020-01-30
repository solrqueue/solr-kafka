package com.github.solrqueue.solrkafka;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.metrics.SolrMetricManager.GaugeWrapper;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.update.processor.AtomicUpdateDocumentMerger;
import org.apache.solr.update.processor.DistributedUpdateProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The consumer counterpart to {@link KafkaProducerUpdateProcessor}. Each consumed record will use
 * an instance of this UpdateProcessor to resume the work of processing the update. In particular,
 * calling processAdd or processDelete on the rest of the UpdateRequestProcessorChain.
 */
public class KafkaConsumerUpdateProcessor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String GAUGE = "UPDATE.kafkaUpdateProcessor.offset";
  private String offsetField;
  protected SolrCore core;
  private AtomicLong currentOffset;
  private AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Get the core associated with this processor
   *
   * @return the core
   */
  public SolrCore getCore() {
    return core;
  }

  /**
   * Create a processor associated with a specific core
   *
   * @param core the core to process updates for
   * @param offsetField the name of the field to record the kafka offset on each created/updated doc
   */
  public KafkaConsumerUpdateProcessor(SolrCore core, String offsetField) {
    this.core = core;
    this.offsetField = offsetField;
    this.currentOffset = new AtomicLong(0L);
    if (!core.getMetricRegistry().getGauges().containsKey(GAUGE)) {
      core.getMetricRegistry()
          .register(GAUGE, new GaugeWrapper<>(() -> currentOffset.get(), "offset"));
    }

    core.addCloseHook(
        new CloseHook() {
          @Override
          public void preClose(SolrCore core) {
            log.info("preClose called for " + core.getName());
            closed.set(true);
          }

          @Override
          public void postClose(SolrCore core) {
            log.info("postClose called for " + core.getName());
          }
        });
  }

  public void setCurrentOffset(Long offset) {
    this.currentOffset.set(offset);
  }

  /**
   * Top-level per-Record consumer entry point. Determines the Record type (Add, Delete, etc), and
   * resumes the processing chain on the consumer. Typically, the rest of the chain is just the
   * local direct indexing processor at this point
   *
   * @param record the Record from Kafka to process
   * @throws IOException if there is an error during processing
   */
  public void process(ConsumerRecord<String, byte[]> record) throws IOException {
    if (closed.get() || core.isClosed()) {
      log.warn("Not processing record {}. Core {} is closing", record.key(), core.getName());
      return;
    }

    String className = KafkaSerializableUpdateCommand.extractClassName(record.headers());
    if (className == null) {
      return;
    }
    currentOffset.set(record.offset());

    if (className.equals(KafkaAddUpdateCommand.class.getName())) {
      AddUpdateCommand cmd = new KafkaAddUpdateCommand(core, record).getCommand();
      processAdd(record, cmd);
      log.debug("processed add {}", cmd);
    } else if (className.equals(KafkaDeleteUpdateCommand.class.getName())) {
      DeleteUpdateCommand cmd = new KafkaDeleteUpdateCommand(core, record).getCommand();
      processDelete(cmd);
      log.debug("processed delete {}", cmd);
    } else {
      log.error("Unexpected class {}", className);
    }
  }

  private void processAdd(ConsumerRecord<String, byte[]> record, AddUpdateCommand cmd)
      throws IOException {
    log.debug("Deserialized cmd {}", cmd);

    if (getUpdatedDocument(cmd)) {
      log.debug("Cmd was an update {}", cmd);
    }
    cmd.solrDoc.setField(offsetField, record.offset());
    log.debug("doing processing {}", cmd);
    processorForCommand(cmd).processAdd(cmd);
  }

  // Adapted from DistributedUpdateProcessor.getUpdatedDocument
  private boolean getUpdatedDocument(AddUpdateCommand cmd) throws IOException {
    long versionOnUpdate = cmd.getVersion();
    if (versionOnUpdate == 0) {
      SolrInputField versionField = cmd.getSolrInputDocument().getField(CommonParams.VERSION_FIELD);
      if (versionField != null) {
        Object o = versionField.getValue();
        versionOnUpdate =
            o instanceof Number ? ((Number) o).longValue() : Long.parseLong(o.toString());
      } else {
        // Find the version
        String versionOnUpdateS = cmd.getReq().getParams().get(CommonParams.VERSION_FIELD);
        versionOnUpdate = versionOnUpdateS == null ? 0 : Long.parseLong(versionOnUpdateS);
      }
    }

    if (!AtomicUpdateDocumentMerger.isAtomicUpdate(cmd)) return false;

    AtomicUpdateDocumentMerger docMerger = new AtomicUpdateDocumentMerger(cmd.getReq());
    Set<String> inPlaceUpdatedFields =
        AtomicUpdateDocumentMerger.computeInPlaceUpdatableFields(cmd);
    if (inPlaceUpdatedFields.size() > 0) { // non-empty means this is suitable for in-place updates
      if (docMerger.doInPlaceUpdateMerge(cmd, inPlaceUpdatedFields)) {
        return true;
      } else {
        // in-place update failed, so fall through and re-try the same with a full atomic update
      }
    }

    // full (non-inplace) atomic update
    SolrInputDocument sdoc = cmd.getSolrInputDocument();
    BytesRef id = cmd.getIndexedId();
    SolrInputDocument oldDoc = RealTimeGetComponent.getInputDocument(cmd.getReq().getCore(), id);

    if (oldDoc == null) {
      // create a new doc by default if an old one wasn't found
      if (versionOnUpdate <= 0) {
        oldDoc = new SolrInputDocument();
      } else {
        // could just let the optimistic locking throw the error
        log.error("Document not found for update.  id=" + cmd.getPrintableId());
      }
    } else {
      oldDoc.remove(CommonParams.VERSION_FIELD);
    }
    cmd.solrDoc = docMerger.merge(sdoc, oldDoc);
    return true;
  }

  private void processDelete(DeleteUpdateCommand cmd) throws IOException {
    log.info("doing processing {}", cmd);
    processorForCommand(cmd).processDelete(cmd);
  }

  private UpdateRequestProcessor processorForCommand(UpdateCommand cmd) {
    SolrQueryRequest req = cmd.getReq();
    SolrQueryResponse rsp = new SolrQueryResponse();
    UpdateRequestProcessor processor =
        restOfChain(core.getUpdateProcessorChain(cmd.getReq().getParams()))
            .createProcessor(req, rsp);
    log.debug("doing processing {}", cmd);
    return processor;
  }

  /**
   * Find the tail of the chain, after KafkaUpdateProcessorFactory, and
   * DistributedUpdateProcessorFactory
   *
   * @param chain the current chain (may be determined by request params)
   * @return a new UpdateRequestProcessorChain containing the tail of the chain
   */
  UpdateRequestProcessorChain restOfChain(UpdateRequestProcessorChain chain) {
    List<UpdateRequestProcessorFactory> factories = new ArrayList<>();
    boolean foundKafkaProcessorFactory = false;
    for (UpdateRequestProcessorFactory factory : chain.getProcessors()) {
      if (factory.getClass().getName().equals(KafkaUpdateProcessorFactory.class.getName())) {
        foundKafkaProcessorFactory = true;
        continue;
      }
      if (factory.getClass().getName().equals(DistributedUpdateProcessorFactory.class.getName())) {
        if (!foundKafkaProcessorFactory) {
          throw new RuntimeException(
              "Unexpected chain order, KafkaProcessorFactory not found before Distributed");
        }
        continue;
      }
      if (foundKafkaProcessorFactory) {
        factories.add(factory);
      }
    }
    if (!foundKafkaProcessorFactory) {
      throw new RuntimeException("Unexpected chain order, KafkaProcessorFactory not found");
    }
    return new UpdateRequestProcessorChain(factories, core);
  }
}
