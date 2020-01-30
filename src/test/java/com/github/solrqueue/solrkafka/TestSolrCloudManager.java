package com.github.solrqueue.solrkafka;

import java.io.IOException;
import java.util.Map;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.TimeSource;

public class TestSolrCloudManager implements SolrCloudManager {
  public ClusterStateProvider clusterStateProvider;

  @Override
  public ClusterStateProvider getClusterStateProvider() {
    return clusterStateProvider;
  }

  @Override
  public NodeStateProvider getNodeStateProvider() {
    return null;
  }

  @Override
  public DistribStateManager getDistribStateManager() {
    return null;
  }

  @Override
  public DistributedQueueFactory getDistributedQueueFactory() {
    return null;
  }

  @Override
  public ObjectCache getObjectCache() {
    return null;
  }

  @Override
  public TimeSource getTimeSource() {
    return null;
  }

  @Override
  public SolrResponse request(SolrRequest solrRequest) throws IOException {
    return null;
  }

  @Override
  public byte[] httpRequest(
      String s, SolrRequest.METHOD method, Map<String, String> map, String s1, int i, boolean b)
      throws IOException {
    return new byte[0];
  }

  @Override
  public void close() throws IOException {}
}
