package com.github.solrqueue.solrbenchmark;

import com.codahale.metrics.Timer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentIndexer {
  private static Logger logger = LoggerFactory.getLogger(DocumentIndexer.class);
  private SolrClient client;
  private String collection;
  private int commitWithinMs;
  private Set<Entry<String, String>> args;
  private ExecutorService executor;

  public DocumentIndexer(
      ExecutorService executor,
      SolrClient client,
      int commitWithinMs,
      String collection,
      Map<String, String> args) {
    this.executor = executor;
    this.args = args.entrySet();
    this.collection = collection;
    this.commitWithinMs = commitWithinMs;
    this.client = client;
  }

  private void doUpdate(SolrInputDocument doc) {
    try {
      UpdateRequest req = new UpdateRequest();
      req.add(doc);
      for (Map.Entry<String, String> arg : args) {
        req.setParam(arg.getKey(), arg.getValue());
      }
      logger.debug("req {}", req);
      try (Timer.Context c = Stats.IndexRequestTimer.time()) {
        UpdateResponse res = req.process(client, collection);
        logger.debug("res {}", res);
      }
      Stats.Indexed.inc();
    } catch (Exception e) {
      throw new RuntimeException("Failed to insert doc", e);
    }
  }

  public void update(SolrInputDocument doc) {
    executor.execute(() -> doUpdate(doc));
  }
}
