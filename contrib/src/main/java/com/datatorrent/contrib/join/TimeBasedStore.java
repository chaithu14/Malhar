/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.join;

import com.datatorrent.lib.bucket.Event;
import com.google.common.collect.Lists;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import javax.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base implementation of time based store for key-value pair tuples.
 * @param <T>
 */
public class TimeBasedStore<T extends TimeEvent>
{
  private static transient final Logger logger = LoggerFactory.getLogger(TimeBasedStore.class);
  @Min(1)
  protected int noOfBuckets;
  protected Bucket<T>[] buckets;
  @Min(1)
  protected long expiryTimeInMillis;
  @Min(1)
  protected long spanTimeInMillis;
  protected int bucketSpanInMillis ;
  protected long startOfBucketsInMillis;
  protected long endOBucketsInMillis;
  private final transient Lock lock;
  private Map<Object, List<Long>> key2Buckets = new HashMap<Object, List<Long>>();
  private transient Timer bucketSlidingTimer;
  private long expiryTime;
  private boolean isOuter=false;
  private transient List<T> unmatchedEvents = new ArrayList<T>();

  protected Map<Long, Bucket> dirtyBuckets = new HashMap<Long, Bucket>();

  public TimeBasedStore()
  {
    lock = new Lock();
  }

  /**
   * Compute the number of buckets based on spantime and bucketSpanInMillis
   */
  private void recomputeNumBuckets()
  {
    Calendar calendar = Calendar.getInstance();
    long now = calendar.getTimeInMillis();
    startOfBucketsInMillis = now - spanTimeInMillis;
    expiryTime = startOfBucketsInMillis;
    expiryTimeInMillis = startOfBucketsInMillis;
    endOBucketsInMillis = now;
    noOfBuckets = (int) Math.ceil((now - startOfBucketsInMillis) / (bucketSpanInMillis * 1.0));
    buckets = (Bucket<T>[]) Array.newInstance(Bucket.class, noOfBuckets);
  }

  public void setup()
  {
    if(buckets == null) {
      recomputeNumBuckets();
    }
    startService();
  }

  /**
   * Return the tuples which satisfies the join constraint
   * @param tuple
   * @return
   */
  public Object getValidTuples(T tuple)
  {
    // Get the key from the given tuple
    Object key = tuple.getEventKey();
    // Get the buckets where the key is present
    List<Long> keyBuckets = key2Buckets.get(key);
    if(keyBuckets == null) {
      return null;
    }
    List<Event> validTuples = new ArrayList<Event>();
    Boolean[] isContained = new Boolean[noOfBuckets];
    Arrays.fill(isContained, false);
    Boolean isContainedInDB = false;
    for(Long idx: keyBuckets) {
      // For the dirty bucket, check whether the time constraint is matching or not
      if(dirtyBuckets.get(idx) != null && isContainedInDB) {
        isContainedInDB = true;
        Bucket tb = (Bucket)dirtyBuckets.get(idx);
        List<T> events = tb.get(key);
        if(events != null) {
          for(T event: events) {
            if(Math.abs(tuple.getTime() - event.getTime()) < spanTimeInMillis) {
              validTuples.add(event);
            }
          }
        }
      } else {
        int bucketIdx = (int) (idx % noOfBuckets);
        if(isContained[bucketIdx])
          continue;
        isContained[bucketIdx] = true;
        Bucket tb = (Bucket)buckets[bucketIdx];
        if(tb == null) {
          return validTuples;
        }
        List<T> events = tb.get(key);
        if(events != null) {
          validTuples.addAll(events);
        }
      }
    }
    return validTuples;
  }

  /**
   * Insert the given tuple into the bucket
   * @param tuple
   */
  public void put(T tuple)
  {
    long bucketKey = getBucketKeyFor(tuple);
    newEvent(bucketKey, tuple);
  }

  /**
   * Calculates the bucket key for the given event
   * @param event
   * @return
   */
  public long getBucketKeyFor(T event)
  {
    long eventTime = event.getTime();
    // Negative indicates the invalid events
    if (eventTime < expiryTimeInMillis) {
      return -1;
    }
    long diffFromStart = eventTime - startOfBucketsInMillis;
    long key = diffFromStart / bucketSpanInMillis;
    synchronized (lock) {
      if (eventTime > endOBucketsInMillis) {
        long move = ((eventTime - endOBucketsInMillis) / bucketSpanInMillis + 1) * bucketSpanInMillis;
        expiryTimeInMillis += move;
        endOBucketsInMillis += move;
      }
    }
    return key;
  }

  private static class Lock
  {
  }

  /**
   * Insert the event into the specified bucketKey
   * @param bucketKey
   * @param event
   */
  public void newEvent(long bucketKey, T event)
  {
    int bucketIdx = (int) (bucketKey % noOfBuckets);

    Bucket<T> bucket = buckets[bucketIdx];

    if (bucket == null || bucket.bucketKey != bucketKey) {
      // If the bucket is already present then the bucket is expirable
      if(bucket != null) {
        dirtyBuckets.put(bucket.bucketKey, bucket);
      }
      bucket = createBucket(bucketKey);
      buckets[bucketIdx] = bucket;
    }

    // Insert the key into the key2Buckets map
    Object key = bucket.getEventKey(event);
    List<Long> keyBuckets = key2Buckets.get(key);
    if(keyBuckets == null) {
      key2Buckets.put(key, Lists.newArrayList(bucketKey));
    } else {
      key2Buckets.get(key).add(bucketKey);
    }
    bucket.addNewEvent(key, event);
  }

  /**
   * Delete the expired buckets at every bucketSpanInMillis periodically
   */
  public void startService()
  {
    bucketSlidingTimer = new Timer();
    endOBucketsInMillis = expiryTime + (noOfBuckets * bucketSpanInMillis);
    logger.debug("bucket properties {}, {}", spanTimeInMillis, bucketSpanInMillis);
    logger.debug("bucket time params: start {}, end {}", startOfBucketsInMillis, endOBucketsInMillis);

    bucketSlidingTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        long time = 0;
        synchronized (lock) {
          time = (expiryTime += bucketSpanInMillis);
          endOBucketsInMillis += bucketSpanInMillis;
        }
        deleteExpiredBuckets(time);
      }
    }, bucketSpanInMillis, bucketSpanInMillis);
  }

  /**
   * Remove the expired buckets. Check the endOfExpired BucketInMillis < expired time
   * @param time
   */
  void deleteExpiredBuckets(long time) {
    Iterator<Long> iterator = dirtyBuckets.keySet().iterator();
    for (; iterator.hasNext(); ) {
      long key = iterator.next();
      Bucket t = (Bucket)dirtyBuckets.get(key);
      if(startOfBucketsInMillis + (t.bucketKey * noOfBuckets) < time) {
        deleteBucket(t);
        iterator.remove();
      }
    }

  }

  /**
   * Return the unmatched events which are present in the expired buckets
   * @return
   */
  public List<T> getUnmatchedEvents()
  {
    List<T> copyEvents = new ArrayList<T>(unmatchedEvents);
    unmatchedEvents.clear();
    return copyEvents;
  }

  /**
   * Delete the given bucket
   * @param bucket
   */
  private void deleteBucket(Bucket bucket) {
    if(bucket == null) {
      return;
    }
    Map<Object, List<T>> writtens = bucket.getEvents();
    if(writtens == null) {
      return;
    }

    for(Map.Entry<Object, List<T>> e: writtens.entrySet()) {
      // Check the events which are unmatched and add those into the unmatchedEvents list
      if(isOuter) {
        for (T event : e.getValue()) {
          if (!((TimeEvent) (event)).isMatch()) {
            unmatchedEvents.add(event);
          }
        }
      }
      key2Buckets.get(e.getKey()).remove(bucket.bucketKey);
      if(key2Buckets.get(e.getKey()).size() == 0) {
        key2Buckets.remove(e.getKey());
      }
    }
  }

  /**
   * Create the bucket with the given key
   * @param bucketKey
   * @return
   */
  protected Bucket<T> createBucket(long bucketKey)
  {
    return new Bucket<T>(bucketKey);
  }

  public void setSpanTimeInMillis(long spanTimeInMillis)
  {
    this.spanTimeInMillis = spanTimeInMillis;
  }

  public void setBucketSpanInMillis(int bucketSpanInMillis)
  {
    this.bucketSpanInMillis = bucketSpanInMillis;
  }

  public void shutdown()
  {
    bucketSlidingTimer.cancel();
  }

  public void isOuterJoin(boolean isOuter)
  {
    this.isOuter = isOuter;
  }
}