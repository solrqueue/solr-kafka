package org.apache.kafka.clients.admin;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;

public class MockAdminClient extends AdminClient {
  public Map<String, TopicListing> topicList;
  public int createdTopics = 0;
  public int createdReplicas = 0;
  public int createdPartitions = 0;
  public int deletedTopics = 0;

  @Override
  public void close(long l, TimeUnit timeUnit) {}

  @Override
  public CreateTopicsResult createTopics(
      Collection<NewTopic> collection, CreateTopicsOptions createTopicsOptions) {
    createdTopics += collection.size();
    Map<String, KafkaFuture<Void>> results =
        collection
            .stream()
            .collect(Collectors.toMap(NewTopic::name, nt -> KafkaFuture.completedFuture(null)));
    createdPartitions = collection.stream().collect(Collectors.summingInt(NewTopic::numPartitions));
    createdReplicas =
        collection.stream().mapToInt(nt -> nt.numPartitions() * nt.replicationFactor()).sum();
    return new CreateTopicsResult(results);
  }

  @Override
  public DeleteTopicsResult deleteTopics(
      Collection<String> collection, DeleteTopicsOptions deleteTopicsOptions) {
    deletedTopics += collection.size();
    Map<String, KafkaFuture<Void>> results =
        collection
            .stream()
            .collect(
                Collectors.toMap(Function.identity(), nt -> KafkaFuture.completedFuture(null)));
    return new DeleteTopicsResult(results);
  }

  @Override
  public ListTopicsResult listTopics(ListTopicsOptions listTopicsOptions) {
    return new ListTopicsResult(KafkaFuture.completedFuture(topicList));
  }

  @Override
  public DescribeTopicsResult describeTopics(
      Collection<String> collection, DescribeTopicsOptions describeTopicsOptions) {
    return null;
  }

  @Override
  public DescribeClusterResult describeCluster(DescribeClusterOptions describeClusterOptions) {
    return null;
  }

  @Override
  public DescribeAclsResult describeAcls(
      AclBindingFilter aclBindingFilter, DescribeAclsOptions describeAclsOptions) {
    return null;
  }

  @Override
  public CreateAclsResult createAcls(
      Collection<AclBinding> collection, CreateAclsOptions createAclsOptions) {
    return null;
  }

  @Override
  public DeleteAclsResult deleteAcls(
      Collection<AclBindingFilter> collection, DeleteAclsOptions deleteAclsOptions) {
    return null;
  }

  @Override
  public DescribeConfigsResult describeConfigs(
      Collection<ConfigResource> collection, DescribeConfigsOptions describeConfigsOptions) {
    return null;
  }

  @Override
  public AlterConfigsResult alterConfigs(
      Map<ConfigResource, Config> map, AlterConfigsOptions alterConfigsOptions) {
    return null;
  }

  @Override
  public AlterReplicaLogDirsResult alterReplicaLogDirs(
      Map<TopicPartitionReplica, String> map,
      AlterReplicaLogDirsOptions alterReplicaLogDirsOptions) {
    return null;
  }

  @Override
  public DescribeLogDirsResult describeLogDirs(
      Collection<Integer> collection, DescribeLogDirsOptions describeLogDirsOptions) {
    return null;
  }

  @Override
  public DescribeReplicaLogDirsResult describeReplicaLogDirs(
      Collection<TopicPartitionReplica> collection,
      DescribeReplicaLogDirsOptions describeReplicaLogDirsOptions) {
    return null;
  }

  @Override
  public CreatePartitionsResult createPartitions(
      Map<String, NewPartitions> map, CreatePartitionsOptions createPartitionsOptions) {
    return null;
  }

  @Override
  public DeleteRecordsResult deleteRecords(
      Map<TopicPartition, RecordsToDelete> map, DeleteRecordsOptions deleteRecordsOptions) {
    return null;
  }

  @Override
  public CreateDelegationTokenResult createDelegationToken(
      CreateDelegationTokenOptions createDelegationTokenOptions) {
    return null;
  }

  @Override
  public RenewDelegationTokenResult renewDelegationToken(
      byte[] bytes, RenewDelegationTokenOptions renewDelegationTokenOptions) {
    return null;
  }

  @Override
  public ExpireDelegationTokenResult expireDelegationToken(
      byte[] bytes, ExpireDelegationTokenOptions expireDelegationTokenOptions) {
    return null;
  }

  @Override
  public DescribeDelegationTokenResult describeDelegationToken(
      DescribeDelegationTokenOptions describeDelegationTokenOptions) {
    return null;
  }

  @Override
  public DescribeConsumerGroupsResult describeConsumerGroups(
      Collection<String> collection, DescribeConsumerGroupsOptions describeConsumerGroupsOptions) {
    return null;
  }

  @Override
  public ListConsumerGroupsResult listConsumerGroups(
      ListConsumerGroupsOptions listConsumerGroupsOptions) {
    return null;
  }

  @Override
  public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(
      String s, ListConsumerGroupOffsetsOptions listConsumerGroupOffsetsOptions) {
    return null;
  }

  @Override
  public DeleteConsumerGroupsResult deleteConsumerGroups(
      Collection<String> collection, DeleteConsumerGroupsOptions deleteConsumerGroupsOptions) {
    return null;
  }
}
