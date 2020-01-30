package com.github.solrqueue.solrkafka;

import java.lang.invoke.MethodHandles;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Diverts UpdateRequests (eg adds, deletes) to Kafka. Alternative to using the
 * DistributedUpdateRequestProcessor, which sends to all replicas via HTTP.
 *
 * <p>The KafkaUpdateProcessorFactory should be placed in the updateProcessorChain directly before
 * DistributedUpdateRequestProcessor:
 *
 * <pre>
 * &lt;processor class="com.github.solrqueue.solrkafka.KafkaUpdateProcessorFactory"&gt;
 *
 *   &lt;!-- where to find your kafka servers --&gt;
 *   &lt;str name="bootstrap.servers"&gt;localhost:9093,localhost:9094&lt;/str&gt;
 *
 *   &lt;!-- name of field on each solr doc to store kafka offset, must be a docValues plong --&gt;
 *   &lt;str name="field"&gt;_offset_&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 *
 * <p>It both forwards the updates to Kafka, and starts a local Consumer that polls for Records that
 * are associated with the cores on this node.
 */
public class KafkaUpdateProcessorFactory extends UpdateRequestProcessorFactory
    implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** <code>kafka.skip</code> param used for controlling whether the Kafka producer is active */
  public static String KAFKA_SKIP_PARAM = "kafka.skip";

  private String bootstrapServers;
  private String offsetField;
  private Producer<String, byte[]> producer;
  private ConcurrentMap<SolrCore, Producer<String, byte[]>> producerAssignments =
      new ConcurrentHashMap<>();
  private Properties producerConfig;
  private boolean skipDefault = false;

  @Override
  public void init(NamedList args) {
    Object skipDefaultConfig = args.remove(KAFKA_SKIP_PARAM);
    if (skipDefaultConfig != null) {
      skipDefault = Boolean.parseBoolean(skipDefaultConfig.toString());
    }
    Object bootstrapServersConfig = args.remove(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    Object offsetFieldConfig = args.remove("field");
    if (bootstrapServersConfig == null) {
      throw new IllegalArgumentException(
          getClass().getName()
              + "is missing required property "
              + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    }
    if (offsetFieldConfig == null) {
      throw new IllegalArgumentException(
          getClass().getName() + "is missing required property field");
    }
    bootstrapServers = bootstrapServersConfig.toString();
    offsetField = offsetFieldConfig.toString();
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    for (String configName : ProducerConfig.configNames()) {
      Object val = args.remove("producer." + configName);
      if (val != null) {
        props.put(configName, val);
      }
    }
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerConfig = props;
    super.init(args);
  }

  /**
   * Called whenever a new core is added. Starts the consumer for that core.
   *
   * @param solrCore the added core
   */
  @Override
  public void inform(SolrCore solrCore) {
    log.info("Registering core " + solrCore.getName());
    KafkaUpdateConsumer.getInstance(solrCore.getCoreContainer(), bootstrapServers, offsetField)
        .reassign();

    solrCore.addCloseHook(
        new CloseHook() {
          @Override
          public void preClose(SolrCore core) {
            log.info("preClose called for " + solrCore.getName());
          }

          @Override
          public void postClose(SolrCore core) {
            log.info("postClose called for " + solrCore.getName());
            synchronized (this) {
              if (producer != null) {
                producerAssignments.remove(solrCore);
                if (producerAssignments.isEmpty() || core.getCoreContainer().isShutDown()) {
                  log.info("Closing producer for " + solrCore.getName());
                  producer.close(1000, TimeUnit.MILLISECONDS);
                  log.info("Closed producer for " + solrCore.getName());
                  producer = null;
                  KafkaUpdateConsumer.getInstance(
                          solrCore.getCoreContainer(), bootstrapServers, offsetField)
                      .shutdown();
                }
              }
            }
          }
        });
  }

  /**
   * Gets the {@link KafkaProducerUpdateProcessor} for an UpdateRequest
   *
   * @param req the update request
   * @param rsp response object to fill
   * @param next next processor in chain
   * @return instance of of {@link KafkaProducerUpdateProcessor} to use
   */
  @Override
  public UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    if (producer == null) {
      synchronized (this) {
        if (producer == null) {
          log.info("Creating producer");
          producer = KafkaClientFactory.INSTANCE.producer(producerConfig);
        }
      }
    }

    /* We'll use this mapping as a kind of reference counter
     * when the core is closed, we'll remove the mapping.
     * when the number of mappings goes to 0, we'll remove
     * the producer
     */
    producerAssignments.putIfAbsent(req.getCore(), producer);
    return new KafkaProducerUpdateProcessor(producer, req, rsp, next, skipDefault);
  }
}
