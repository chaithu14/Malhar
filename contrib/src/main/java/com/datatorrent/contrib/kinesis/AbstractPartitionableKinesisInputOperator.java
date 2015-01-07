/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.kinesis;

import com.amazonaws.services.kinesis.model.Shard;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Min;
import java.util.*;

@OperatorAnnotation(partitionable = true)
public abstract class AbstractPartitionableKinesisInputOperator extends AbstractKinesisInputOperator<KinesisConsumer> implements Partitioner<AbstractPartitionableKinesisInputOperator>, StatsListener
{
  // By default the partition policy is 1:1
  public PartitionStrategy strategy = PartitionStrategy.ONE_TO_ONE;

  private transient OperatorContext context = null;

  private static final Logger logger = LoggerFactory.getLogger(AbstractPartitionableKinesisInputOperator.class);

  // Store the current partition info
  private transient Set<PartitionInfo> currentPartitionInfo = new HashSet<PartitionInfo>();

  private ShardManager shardManager = null;

  // Minimal interval between 2 (re)partition actions
  private long repartitionInterval = 30000L;

  // Minimal interval between checking collected stats and decide whether it needs to repartition or not.
  private long repartitionCheckInterval = 5000L;

  private transient long lastCheckTime = 0L;

  private transient long lastRepartitionTime = 0L;

  //No of shards per partition in dynamic MANY_TO_ONE strategy
  @Min(1)
  private Integer shardsPerPartition = 1;

  private boolean isDynamicPartition = false;

  @Min(1)
  private int initialPartitionCount = 1;

  private transient List<String> newWaitingPartition = new LinkedList<String>();

  @Override
  public void partitioned(Map<Integer, Partition<AbstractPartitionableKinesisInputOperator>> partitions)
  {
    // update the last repartition time
    lastRepartitionTime = System.currentTimeMillis();
  }

  @Override
  public Collection<Partition<AbstractPartitionableKinesisInputOperator>> definePartitions(Collection<Partition<AbstractPartitionableKinesisInputOperator>> partitions, int incrementalCapacity)
  {
    boolean isInitialParitition = partitions.iterator().next().getStats() == null;
    // Set the credentials to get the list of shards
    if(isInitialParitition) {
      try {
        KinesisUtil.setAWSCredentials(credentialsProvider);
      } catch (Exception e) {
        logger.error(e.getMessage());
        throw new RuntimeException(e.getMessage());
      }
    }
    List<Shard> shards = KinesisUtil.getShardList(getStreamName());

    // Operator partitions
    List<Partition<AbstractPartitionableKinesisInputOperator>> newPartitions = null;

    // initialize the shard positions
    Map<String, String> initShardPos = null;
    if(isInitialParitition && shardManager !=null){
      initShardPos = shardManager.loadInitialShardPositions();
    }

    switch (strategy) {
    // For the 1 to 1 mapping The framework will create number of operator partitions based on kinesis shards
    // Each operator partition will consume from only one kinesis shard
    case ONE_TO_ONE:
      if (isInitialParitition) {
        lastRepartitionTime = System.currentTimeMillis();
        logger.info("[ONE_TO_ONE]: Initializing partition(s)");
        // initialize the number of operator partitions according to number of shards
        newPartitions = new ArrayList<Partition<AbstractPartitionableKinesisInputOperator>>(shards.size());
        for (int i = 0; i < shards.size(); i++) {
          logger.info("[ONE_TO_ONE]: Create operator partition for kinesis partition: " + shards.get(i).getShardId() + ", StreamName: " + this.getConsumer().streamName);
          newPartitions.add(createPartition(Sets.newHashSet(shards.get(i).getShardId()), initShardPos));
        }
      } else if (newWaitingPartition.size() != 0) {
        // Remove the partitions for the closed shards
        RemovePartitions(partitions);
        // add partition for new kinesis shard
        for (String pid : newWaitingPartition) {
          logger.info("[ONE_TO_ONE]: Add operator partition for kinesis partition " + pid);
          partitions.add(createPartition(Sets.newHashSet(pid), null));
        }
        newWaitingPartition.clear();
        return partitions;
      }
      break;
    // For the N to 1 mapping The initial partition number is defined by stream application
    // Afterwards, the framework will dynamically adjust the partition
    case MANY_TO_ONE:
      /* This case was handled into two ways.
         1. Dynamic Partition: Number of DT partitions is depends on the number of open shards.
         2. Static Partition: Number of DT partitions is fixed, whether the number of shards are increased/decreased.
      */
      int size = initialPartitionCount;
      if(newWaitingPartition.size() != 0)
      {
        // Get the list of open shards
        shards = getOpenShards(partitions);
        if(isDynamicPartition)
          size = (int)Math.ceil(shards.size() / (shardsPerPartition * 1.0));
        initShardPos = shardManager.loadInitialShardPositions();
      }
      @SuppressWarnings("unchecked")
      Set<String>[] pIds = new Set[size];
      newPartitions = new ArrayList<Partition<AbstractPartitionableKinesisInputOperator>>(size);
      for (int i = 0; i < shards.size(); i++) {
        Shard pm = shards.get(i);
        if (pIds[i % size] == null) {
          pIds[i % size] = new HashSet<String>();
        }
        pIds[i % size].add(pm.getShardId());
      }
      if (isInitialParitition) {
        lastRepartitionTime = System.currentTimeMillis();
        logger.info("[MANY_TO_ONE]: Initializing partition(s)");
      } else {
        logger.info("[MANY_TO_ONE]: Add operator partition for kinesis partition(s): " + StringUtils.join(newWaitingPartition, ", ") + ", StreamName: " + this.getConsumer().streamName);
        newWaitingPartition.clear();
      }
      for (int i = 0; i < pIds.length; i++) {
        logger.info("[MANY_TO_ONE]: Create operator partition for kinesis partition(s): " + StringUtils.join(pIds[i], ", ") + ", StreamName: " + this.getConsumer().streamName);
        if(pIds[i] != null)
          newPartitions.add(createPartition(pIds[i], initShardPos));
      }
      break;
    default:
      break;
    }
    return newPartitions;
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    Response resp = new Response();
    List<KinesisConsumer.KinesisShardStats> kstats = extractkinesisStats(stats);
    resp.repartitionRequired = needPartition(stats.getOperatorId(), kstats);
    return resp;
  }

  private void updateShardPositions(List<KinesisConsumer.KinesisShardStats> kstats)
  {
    //In every partition check interval, call shardmanager to update the positions
    if (shardManager != null) {
      shardManager.updatePositions(KinesisConsumer.KinesisShardStatsUtil.getShardStatsForPartitions(kstats));
    }
  }

  private List<KinesisConsumer.KinesisShardStats> extractkinesisStats(BatchedOperatorStats stats)
  {
    //preprocess the stats
    List<KinesisConsumer.KinesisShardStats> kmsList = new LinkedList<KinesisConsumer.KinesisShardStats>();
    for (Stats.OperatorStats os : stats.getLastWindowedStats()) {
      if (os != null && os.counters instanceof KinesisConsumer.KinesisShardStats) {
        kmsList.add((KinesisConsumer.KinesisShardStats) os.counters);
      }
    }
    return kmsList;
  }

  private boolean needPartition(int opid,  List<KinesisConsumer.KinesisShardStats> kstats)
  {

    long t = System.currentTimeMillis();

    if (t - lastCheckTime < repartitionCheckInterval) {
      // return false if it's within repartitionCheckInterval since last time it check the stats
      return false;
    }
    logger.debug("Use ShardManager to update the Shard Positions");
    updateShardPositions(kstats);
    if(repartitionInterval < 0){
      // if repartition is disabled
      return false;
    }

    if(t - lastRepartitionTime < repartitionInterval) {
      // return false if it's still within repartitionInterval since last (re)partition
      return false;
    }

    try {
      // monitor if shards are repartitioned
      Set<String> existingIds = new HashSet<String>();
      for (PartitionInfo pio : currentPartitionInfo) {
        existingIds.addAll(pio.kpids);
      }
      List<Shard> shards = KinesisUtil.getShardList(getStreamName());
      for (Shard shard :shards) {
        if (!existingIds.contains(shard.getShardId())) {
          newWaitingPartition.add(shard.getShardId());
        }
      }

      if (newWaitingPartition.size() != 0) {
        // found new kinesis partition
        lastRepartitionTime = t;
        return true;
      }
      return false;
    } finally {
      // update last  check time
      lastCheckTime = System.currentTimeMillis();
    }
  }

  private final AbstractPartitionableKinesisInputOperator _cloneOperator()
  {
    AbstractPartitionableKinesisInputOperator newOp = cloneOperator();
    newOp.strategy = this.strategy;
    newOp.setCredentialsProvider(this.credentialsProvider);
    newOp.setStreamName(this.getStreamName());
    return newOp;
  }

  // If all the shards in the partition are closed, then remove that partition
  private void RemovePartitions(Collection<Partition<AbstractPartitionableKinesisInputOperator>> partitions)
  {
    List<Partition<AbstractPartitionableKinesisInputOperator>> closedPartitions = new ArrayList<Partition<AbstractPartitionableKinesisInputOperator>>();
    for(Partition<AbstractPartitionableKinesisInputOperator> op : partitions)
    {
      if(op.getPartitionedInstance().getConsumer().getClosedShards().size() ==
          op.getPartitionedInstance().getConsumer().getNumOfShards())
      {
        closedPartitions.add(op);
      }
    }
    if(closedPartitions.size() != 0)
    {
      for(Partition<AbstractPartitionableKinesisInputOperator> op : closedPartitions)
      {
        partitions.remove(op);
      }
    }
  }

  // Get the list of open shards
  private List<Shard> getOpenShards(Collection<Partition<AbstractPartitionableKinesisInputOperator>> partitions)
  {
    List<Shard> closedShards = new ArrayList<Shard>();
    for(Partition<AbstractPartitionableKinesisInputOperator> op : partitions)
    {
      closedShards.addAll(op.getPartitionedInstance().getConsumer().getClosedShards());
    }
    List<Shard> shards = KinesisUtil.getShardList(getStreamName());
    List<Shard> openShards = new ArrayList<Shard>();
    for (Shard shard :shards) {
      if(!closedShards.contains(shard)) {
        openShards.add(shard);
      }
    }
    return openShards;
  }
  // Create a new partition with the shardIds and initial shard positions
  private
  Partition<AbstractPartitionableKinesisInputOperator> createPartition(Set<String> shardIds, Map<String, String> initShardPos)
  {
    Partition<AbstractPartitionableKinesisInputOperator> p = new DefaultPartition<AbstractPartitionableKinesisInputOperator>(_cloneOperator());
    KinesisConsumer newConsumerForPartition = getConsumer().cloneConsumer(shardIds, initShardPos);
    p.getPartitionedInstance().setConsumer(newConsumerForPartition);
    PartitionInfo pif = new PartitionInfo();
    pif.kpids = shardIds;
    currentPartitionInfo.add(pif);
    return p;
  }
  /**
   * Implement this method to initialize new operator instance for new partition.
   * Please carefully include all the properties you want to keep in new instance
   *
   * @return
   */
  protected abstract AbstractPartitionableKinesisInputOperator cloneOperator();

  @Override
  public void setup(OperatorContext context)
  {

    super.setup(context);
    this.context = context;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    context.setCounters(getConsumer().getConsumerStats());
  }

  public static enum PartitionStrategy
  {
    /**
     * Each operator partition connect to only one kinesis Shard
     */
    ONE_TO_ONE,
    /**
     * Each operator consumes from several Shards.
     */
    MANY_TO_ONE
  }

  public int getInitialPartitionCount()
  {
    return initialPartitionCount;
  }

  public void setInitialPartitionCount(int initialPartitionCount)
  {
    this.initialPartitionCount = initialPartitionCount;
  }

  public long getLastRepartitionTime()
  {
    return lastRepartitionTime;
  }

  public void setLastRepartitionTime(long lastRepartitionTime)
  {
    this.lastRepartitionTime = lastRepartitionTime;
  }

  public long getLastCheckTime()
  {
    return lastCheckTime;
  }

  public void setLastCheckTime(long lastCheckTime)
  {
    this.lastCheckTime = lastCheckTime;
  }

  public void setInitialOffset(String initialOffset)
  {
    this.consumer.initialOffset = initialOffset;
  }

  public void setRepartitionCheckInterval(long repartitionCheckInterval)
  {
    this.repartitionCheckInterval = repartitionCheckInterval;
  }

  public long getRepartitionCheckInterval()
  {
    return repartitionCheckInterval;
  }

  public void setRepartitionInterval(long repartitionInterval)
  {
    this.repartitionInterval = repartitionInterval;
  }

  public long getRepartitionInterval()
  {
    return repartitionInterval;
  }

  public void setStrategy(String policy)
  {
    this.strategy = PartitionStrategy.valueOf(policy.toUpperCase());
  }

  static class PartitionInfo
  {
    Set<String> kpids;
  }

  public Integer getShardsPerPartition()
  {
    return shardsPerPartition;
  }

  public void setShardsPerPartition(Integer shardsPerPartition)
  {
    this.shardsPerPartition = shardsPerPartition;
  }

  public boolean isDynamicPartition()
  {
    return isDynamicPartition;
  }

  public void setDynamicPartition(boolean isDynamicPartition)
  {
    this.isDynamicPartition = isDynamicPartition;
  }

  public ShardManager getShardManager()
  {
    return shardManager;
  }

  public void setShardManager(ShardManager shardManager)
  {
    this.shardManager = shardManager;
  }
}