package com.datatorrent.contrib.kinesis;

/**
 * Created by chaitanya on 22/12/14.
 */
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Pattern.Flag;
import java.io.Closeable;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Base KinesisConsumer class used by kinesis input operator
 *
 */
public class KinesisConsumer implements Closeable
{
  private static final Logger logger = LoggerFactory.getLogger(KinesisConsumer.class);
  protected Integer recordsLimit = 100;

  private int cacheSize = 1024;

  protected transient boolean isAlive = false;

  private transient ArrayBlockingQueue<Record> holdingBuffer;

  /**
   * The streamName that this consumer consumes
   */
  @NotNull
  protected String streamName = "streamName";

  /**
   * The initialOffset could be either earliest or latest
   * Earliest means the beginning the shard
   * Latest means the current record to consume from the shard
   * By default it always consume from the beginning of the shard
   */
  @Pattern(flags={Flag.CASE_INSENSITIVE}, regexp = "earliest|latest")
  protected String initialOffset = "earliest";

  protected transient ExecutorService consumerThreadExecutor = null;

  protected ConcurrentHashMap<String, String> ShardPosition = new ConcurrentHashMap<String, String>();
  protected final transient HashSet<Shard> simpleConsumerThreads = new HashSet<Shard>();
  private Set<String> shardIds = new HashSet<String>();
  protected Set<Shard> closedShards = new HashSet<Shard>();
  protected long recordsCheckInterval = 500;


  protected transient KinesisShardStats stats = new KinesisShardStats();

  public KinesisConsumer()
  {
  }

  public KinesisConsumer(String streamName)
  {
    this.streamName = streamName;
  }

  public KinesisConsumer(String streamName, Set<String> newShardIds)
  {
    this(streamName);
    shardIds = newShardIds;
  }
  /**
   * This method is called in setup method of the operator
   */
  public void create(){
    holdingBuffer = new ArrayBlockingQueue<Record>(cacheSize);
    boolean defaultSelect = (shardIds == null) || (shardIds.size() == 0);
    final List<Shard> pms = KinesisUtil.getShardList(streamName);
    for (final Shard shId: pms) {
      if((shardIds.contains(shId.getShardId()) || defaultSelect) && !closedShards.contains(shId.getShardId())) {
        simpleConsumerThreads.add(shId);
      }
    }
  }

  /**
   * This method returns the iterator type of the given shard
   */
  public ShardIteratorType getIteratorType(String shardId)
  {
    if(ShardPosition.containsKey(shardId)) {
      return ShardIteratorType.AFTER_SEQUENCE_NUMBER;
    }
    return initialOffset.equalsIgnoreCase("earliest") ? ShardIteratorType.TRIM_HORIZON : ShardIteratorType.LATEST;
  }

  /**
   * This method is called in the activate method of the operator
   */
  public void start(){
    isAlive = true;
    int realNumStream =  simpleConsumerThreads.size();
    if(realNumStream == 0)
      return;

    consumerThreadExecutor = Executors.newFixedThreadPool(realNumStream);
    for (final Shard shards : simpleConsumerThreads) {
      consumerThreadExecutor.submit(new Runnable() {
        @Override
        public void run()
        {
          logger.debug("Thread " + Thread.currentThread().getName() + " start consuming Records...");
          while (isAlive ) {
            Shard shId = shards;
            List<Record> records = KinesisUtil.getRecords(streamName, recordsLimit, shId, getIteratorType(shId.getShardId()), ShardPosition.get(shId.getShardId()));
            if (records == null || records.isEmpty()) {
              if(shId.getSequenceNumberRange().getEndingSequenceNumber() != null)
              {
                closedShards.add(shId);
                break;
              }
              try {
                Thread.sleep(recordsCheckInterval);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            } else {
              String seqNo = "";
              for (Record rc : records) {
                try {
                  seqNo = rc.getSequenceNumber();
                  putRecord(rc);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }
              ShardPosition.put(shId.getShardId(), new String(seqNo));

            }
          }
          logger.debug("Thread " + Thread.currentThread().getName() + " stop consuming Records...");
        }
      });
    }
  }

  @Override
  public void close()
  {
    if(consumerThreadExecutor!=null) {
      consumerThreadExecutor.shutdown();
    }
    simpleConsumerThreads.clear();
  }

  /**
   * The method is called in the deactivate method of the operator
   */
  public void stop() {
    isAlive = false;
    holdingBuffer.clear();
    IOUtils.closeQuietly(this);
  }

  protected KinesisConsumer cloneConsumer(Set<String> partitionIds)
  {
    return cloneConsumer(partitionIds, null);
  }

  protected KinesisConsumer cloneConsumer(Set<String> partitionIds, Map<String, String> shardPositions)
  {
    KinesisConsumer newConsumer = new KinesisConsumer(this.streamName, partitionIds);
    newConsumer.initialOffset = this.initialOffset;
    newConsumer.recordsCheckInterval = this.recordsCheckInterval;
    newConsumer.resetShardPositions(shardPositions);
    return newConsumer;
  }

  private void resetShardPositions(Map<String, String> shardPositions){

    if(shardPositions == null){
      return;
    }
    ShardPosition.clear();

    for (String pid: shardIds) {
      String offsetForPar = shardPositions.get(pid);
      if (offsetForPar != null && !offsetForPar.equals("")) {
        ShardPosition.put(pid, offsetForPar);
      }
    }
  }

  protected Map<String, String> getShardPosition()
  {
    return ShardPosition;
  }

  public Set<Shard> getClosedShards()
  {
    return closedShards;
  }

  public Integer getNumOfShards()
  {
    return shardIds.size();
  }

  public KinesisShardStats getConsumerStats()
  {
    stats.updateShardStats(ShardPosition);
    return stats;
  }
  /**
   * This method is called in teardown method of the operator
   */
  public void teardown()
  {
    holdingBuffer.clear();
  }

  public boolean isAlive()
  {
    return isAlive;
  }

  public void setAlive(boolean isAlive)
  {
    this.isAlive = isAlive;
  }

  public void setStreamName(String streamName)
  {
    this.streamName = streamName;
  }

  public String getStreamName()
  {
    return streamName;
  }

  public Record pollRecord()
  {
    return holdingBuffer.poll();
  }

  public int messageSize()
  {
    return holdingBuffer.size();
  }

  public void setInitialOffset(String initialOffset)
  {
    this.initialOffset = initialOffset;
  }

  public String getInitialOffset()
  {
    return initialOffset;
  }

  public int getCacheSize()
  {
    return cacheSize;
  }

  public void setCacheSize(int cacheSize)
  {
    this.cacheSize = cacheSize;
  }

  final protected void putRecord(Record msg) throws InterruptedException{
    holdingBuffer.put(msg);
  };

  public Integer getRecordsLimit()
  {
    return recordsLimit;
  }

  public void setRecordsLimit(Integer recordsLimit)
  {
    this.recordsLimit = recordsLimit;
  }

  /**
   * Counter class which gives the statistic value from the consumer
   */
  public static class KinesisShardStats implements Serializable
  {
    public ConcurrentHashMap<String, String> partitionStats = new ConcurrentHashMap<String, String>();

    public KinesisShardStats()
    {
    }
    //important API for update
    public void updateShardStats(Map<String, String> shardStats){
      for (Entry<String, String> ss : shardStats.entrySet()) {
        partitionStats.put(ss.getKey(), ss.getValue());
      }
    }
  }

  public static class KinesisShardStatsUtil {
    public static Map<String, String> getShardStatsForPartitions(List<KinesisShardStats> kinesisshardStats)
    {
      Map<String, String> result = Maps.newHashMap();
      for (KinesisShardStats kms : kinesisshardStats) {
        for (Entry<String, String> item : kms.partitionStats.entrySet()) {
          result.put(item.getKey(), item.getValue());
        }
      }
      return result;
    }
  }
}