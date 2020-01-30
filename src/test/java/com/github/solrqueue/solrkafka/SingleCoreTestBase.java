package com.github.solrqueue.solrkafka;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.junit.After;
import org.junit.Before;

public class SingleCoreTestBase {
  protected SolrCore testCore;

  protected LocalSolrQueryRequest emptyReq() {
    return new LocalSolrQueryRequest(testCore, Collections.emptyMap());
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

  @Before
  public void setUp() throws Exception {
    File solrHome = new File("src/test/resources/solr_home");
    Path tempDir = Files.createTempDirectory("solrTest");
    FileUtils.copyDirectory(solrHome, tempDir.toFile());
    String newCoreName = "testcore_" + new Random().nextInt();
    FileUtils.moveDirectory(
        tempDir.resolve("testcore").toFile(), tempDir.resolve(newCoreName).toFile());
    SolrResourceLoader loader = new SolrResourceLoader(tempDir);
    CoreContainer container = new CoreContainer(loader);
    container.load();
    container.create(newCoreName, Collections.emptyMap());
    new EmbeddedSolrServer(container, newCoreName);
    testCore = container.getCore(newCoreName);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    FileUtils.deleteDirectory(tempDir.toFile());
                  } catch (IOException ioe) {
                  }
                }));
  }

  @After
  public void tearDown() throws Exception {
    // Manually cleanup the testcore data directory
    if (testCore != null) {
      testCore.closeSearcher();
    }
  }
}
