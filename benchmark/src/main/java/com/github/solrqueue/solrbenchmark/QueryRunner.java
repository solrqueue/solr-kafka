package com.github.solrqueue.solrbenchmark;

import com.codahale.metrics.Timer;
import java.util.concurrent.ExecutorService;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryRunner {
  private static Logger logger = LoggerFactory.getLogger(QueryRunner.class);
  private SolrClient client;
  private ExecutorService executor;
  private String collection;

  public QueryRunner(ExecutorService executor, SolrClient client, String collection) {
    this.executor = executor;
    this.client = client;
    this.collection = collection;
  }

  private void doQuery(SolrParams params, boolean isAll) {
    logger.debug("qreq {}", params);
    try {
      try (Timer.Context c = Stats.QueryRequestTimer.time()) {
        QueryResponse res = client.query(collection, params);
        logger.debug("qres {}", res);
        if (isAll) {
          Stats.AvailableCount.set(res.getResults().getNumFound());
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Query failed", e);
    }
  }

  public void query(String q, boolean isAll) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", q);
    executor.execute(() -> doQuery(params, isAll));
  }

  public void queryAll() {
    query("{!cache=false}*:*", true);
  }
}
