package org.apache.apex.malhar.lib.state.managed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.TimeSlicedBucketedState;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

import com.datatorrent.netlet.util.Slice;

public class ManagedStateMultiValueImpl extends ManagedStateImpl implements TimeSlicedBucketedState
{
  private final transient LinkedBlockingQueue<Long> purgedTimeBuckets = Queues.newLinkedBlockingQueue();
  private final transient Set<Bucket> bucketsForTeardown = Sets.newHashSet();
  private transient Map<Slice, List<Long>> keysInBuckets = new HashMap<>();
  private int noOfKeyBuckets;

  @Override
  public int getNumBuckets()
  {
    noOfKeyBuckets = numBuckets;
    return numBuckets * timeBucketAssigner.getNumBuckets();
  }

  @Override
  public void put(long bucketId, long time, @NotNull Slice key, @NotNull Slice value)
  {
    long timeBucket = (timeBucketAssigner.getTimeBucketAndAdjustBoundaries(time));
    bucketId = (timeBucket * noOfKeyBuckets) + bucketId;
    putInBucket(bucketId, timeBucket, key, value);
  }

  @Override
  public Slice getSync(long bucketId, @NotNull Slice key)
  {
    return getValueFromBucketSync(bucketId, -1, key);
  }

  public Slice getSync(long bucketId, int timeIdx, @NotNull Slice key)
  {
    int bucketIdx = (int)(bucketId + (timeIdx * noOfKeyBuckets));
    Bucket bucket = buckets[bucketIdx];
    if (bucket == null) {
      return null;
    }

    synchronized (bucket) {
      return bucket.get(key, -1, Bucket.ReadSource.ALL);
    }
  }

  @Override
  public Slice getSync(long bucketId, long time, @NotNull Slice key)
  {
    long timeBucket = (timeBucketAssigner.getTimeBucketAndAdjustBoundaries(time));
    bucketId = (timeBucket * noOfKeyBuckets) + bucketId;
    return getValueFromBucketSync(bucketId, timeBucket, key);
  }

  class GetDataTask implements GetAsyncCallback, Future<List<Slice>>
  {
    List<Slice> slices = new ArrayList<>();
    int num;
    int completed;
    GetDataTask(int num)
    {
      this.num  = num;
      this.completed = 0;
    }

    @Override
    public void completed(long bucketId, Slice key, Slice value, Throwable ex)
    {
      synchronized (this) {
        completed++;
        LOG.info("Completed: {} -> {}", bucketId, key);
        if (ex == null && value != null) {
          slices.add(value);
        }
      }
    }

    volatile boolean cancelled = false;
    @Override
    public boolean cancel(boolean b)
    {
      this.cancelled = b;
      return this.cancelled;
    }

    @Override
    public boolean isCancelled()
    {
      return cancelled;
    }

    @Override
    public boolean isDone()
    {
      return (completed == num);
    }

    @Override
    public List<Slice> get() throws InterruptedException, ExecutionException
    {
      while (!isDone() || !isCancelled()) {
        Thread.sleep(1);
      }
      return slices;
    }

    @Override
    public List<Slice> get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException
    {
      while (!isDone() || !isCancelled()) {
        Thread.sleep(1);
      }
      return slices;
    }

  }

  @Override
  public Future<Slice> getAsync(long bucketId, @NotNull Slice key)
  {
    return null;
  }

  public Future<List<Slice>> getAsyncValue(long bucketId, @NotNull Slice key)
  {
    LOG.info("getAsyncValue: {} -> {} -> {}", bucketId, key, timeBucketAssigner.getNumBuckets());
    GetDataTask task = new GetDataTask(timeBucketAssigner.getNumBuckets());
    for (int i = 0; i < timeBucketAssigner.getNumBuckets(); i++)  {
      int bucketIdx = (int)(bucketId + (i * noOfKeyBuckets));
      Bucket bucket = buckets[bucketIdx];

      getValueFromBucketAsync(bucket, key, task);
    }
    LOG.info("End of getAsyncValue: {} -> {}", bucketId, key);
    return task;
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  protected void getValueFromBucketAsync(
      Bucket bucket, @NotNull Slice key, GetAsyncCallback callback)
  {
    if (bucket == null) {
      callback.completed(-1, key, null, null);
      return;
    }

    Preconditions.checkNotNull(key, "key");
    synchronized (bucket) {
      Slice cachedVal = bucket.get(key, -1, Bucket.ReadSource.MEMORY);
      if (cachedVal != null) {
        callback.completed(bucket.getBucketId(), key, cachedVal, null);
      }
      ValueFetchTask valueFetchTask = new ValueFetchTask(bucket, key, callback);
      readerService.submit(valueFetchTask);
    }
  }

  @Override
  public Future<Slice> getAsync(long bucketId, long timeIdx, @NotNull Slice key)
  {
    return null;
  }

  @Override
  public void endWindow()
  {
    LOG.info("End WIndow - 0");
    super.endWindow();
    Long purgedTimeBucket;

    //collect all the purged time buckets
    while (null != (purgedTimeBucket = purgedTimeBuckets.poll())) {
      int purgedTimeBucketIdx = getBucketIdx(purgedTimeBucket);
      if (buckets[purgedTimeBucketIdx] != null && buckets[purgedTimeBucketIdx].getBucketId() == purgedTimeBucket) {
        bucketsForTeardown.add(buckets[purgedTimeBucketIdx]);
        buckets[purgedTimeBucketIdx] = null;
      }
    }

    //tear down all the eligible time buckets
    Iterator<Bucket> bucketIterator = bucketsForTeardown.iterator();
    while (bucketIterator.hasNext()) {
      Bucket bucket = bucketIterator.next();
      if (!tasksPerBucketId.containsKey(bucket.getBucketId())) {
        //no pending asynchronous queries for this bucket id
        bucket.teardown();
        bucketIterator.remove();
      }
    }
    LOG.info("End WIndow - 1");
  }

  @Override
  protected void handleBucketConflict(int bucketIdx, long newBucketId)
  {
    Preconditions.checkArgument(buckets[bucketIdx].getBucketId() < newBucketId, "new time bucket should have a value"
        + " greater than the old time bucket");
    //Time buckets are purged periodically so here a bucket conflict is expected and so we just ignore conflicts.
    bucketsForTeardown.add(buckets[bucketIdx]);
    buckets[bucketIdx] = newBucket(newBucketId);
    buckets[bucketIdx].setup(this);
  }

  @Override
  public void purgeTimeBucketsLessThanEqualTo(long timeBucket)
  {
    purgedTimeBuckets.add(timeBucket);
    super.purgeTimeBucketsLessThanEqualTo(timeBucket);
  }

  interface GetAsyncCallback
  {
    void completed(long bucketId, Slice key, Slice value, Throwable ex);
  }


  public static class ValueFetchTask implements Callable<Slice>
  {
    private final Bucket bucket;
    private final Slice key;
    GetAsyncCallback callback;

    ValueFetchTask(@NotNull Bucket bucket, @NotNull Slice key, GetAsyncCallback callback)
    {
      this.bucket = Preconditions.checkNotNull(bucket);
      this.key = Preconditions.checkNotNull(key);
      this.callback = callback;
    }

    @Override
    public Slice call() throws Exception
    {
      try {
        synchronized (bucket) {
          //a particular bucket should only be handled by one thread at any point of time. Handling of bucket here
          //involves creating readers for the time buckets and de-serializing key/value from a reader.
          Slice value = bucket.get(key, -1, Bucket.ReadSource.ALL);
          if (callback != null) {
            callback.completed(bucket.getBucketId(), key, value, null);
          }
          return value;
        }
      } catch (Throwable t) {
        if (callback != null) {
          callback.completed(bucket.getBucketId(), key, null, t);
        }
        throw Throwables.propagate(t);
      }
    }
  }

  public int getNoOfKeyBuckets()
  {
    return noOfKeyBuckets;
  }

  public void setNoOfKeyBuckets(int noOfKeyBuckets)
  {
    this.noOfKeyBuckets = noOfKeyBuckets;
  }

  private static transient Logger LOG = LoggerFactory.getLogger(ManagedTimeUnifiedStateImpl.class);
}

