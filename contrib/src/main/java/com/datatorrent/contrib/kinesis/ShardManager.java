package com.datatorrent.contrib.kinesis;

/**
 * Created by chaitanya on 23/12/14.
 */
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * An shard manager interface used by  AbstractPartitionableKinesisInputOperator to define the customized initial positions and periodically update the current shard positions of all the operators
 * Ex. you could write shardManager to hdfs and load it back when restart the application
 *
 */
public class ShardManager
{

  private final transient Map<String, String> shardPos = Collections.synchronizedMap(new HashMap<String, String>());
  /**
   * Load initial positions for all kinesis Shards
   * The method is called at the first attempt of creating shards and the return value is used as initial positions for simple consumer
   *
   * @return Map of Kinesis shard id as key and sequence id as value
   */
  public Map<String, String> loadInitialShardPositions()
  {
    return shardPos;
  }

  /**
   * @param shardPositions positions for specified shards, it is reported by individual operator instances
   * The method is called every AbstractPartitionableKinesisInputOperator.getRepartitionCheckInterval() to update the current positions
   */
  public void updatePositions(Map<String, String> shardPositions)
  {
    shardPos.putAll(shardPositions);
  }
}