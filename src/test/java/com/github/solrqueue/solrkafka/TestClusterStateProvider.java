package com.github.solrqueue.solrkafka;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.cloud.ClusterState;

public class TestClusterStateProvider implements ClusterStateProvider {
  public ClusterState clusterState;

  @Override
  public ClusterState.CollectionRef getState(String s) {
    return null;
  }

  @Override
  public Set<String> getLiveNodes() {
    return null;
  }

  @Override
  public List<String> resolveAlias(String s) {
    return null;
  }

  @Override
  public ClusterState getClusterState() throws IOException {
    return clusterState;
  }

  @Override
  public Map<String, Object> getClusterProperties() {
    return null;
  }

  @Override
  public String getPolicyNameByCollection(String s) {
    return null;
  }

  @Override
  public void connect() {}

  @Override
  public void close() throws IOException {}
}
