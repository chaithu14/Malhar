package com.datatorrent.contrib.join;

import com.datatorrent.lib.bucket.Bucketable;
import com.datatorrent.lib.bucket.Event;
import com.google.common.collect.Lists;
import java.lang.reflect.Array;
import java.util.ArrayList;
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

public class TimeBasedStore<T extends Event & Bucketable>
{
  private static transient final Logger logger = LoggerFactory.getLogger(TimeBasedStore.class);
  @Min(1)
  protected int noOfBuckets;
  protected TimeBucket<T>[] buckets;
  protected long expiryTimeInMillis = 1 ;
  protected long spanTime = 1 ;
  protected int bucketSpanInMillis = 30000;
  protected long startOfBucketsInMillis;
  protected long endOBucketsInMillis;
  private final transient Lock lock;
  private Map<Object, List<Long>> key2Buckets;
  private transient Timer bucketSlidingTimer;
  private long expiryTime;
  private boolean isOuter=false;
  private List<T> unmatchedEvents = new ArrayList<T>();

  protected Map<Long, TimeBucket> dirtyBuckets;

  public TimeBasedStore()
  {
    lock = new Lock();
    dirtyBuckets = new HashMap<Long, TimeBucket>();
    key2Buckets = new HashMap<Object, List<Long>>();
  }

  private void recomputeNumBuckets()
  {
    Calendar calendar = Calendar.getInstance();
    long now = calendar.getTimeInMillis();
    startOfBucketsInMillis = now - spanTime;
    expiryTime = startOfBucketsInMillis;
    expiryTimeInMillis = startOfBucketsInMillis;
    endOBucketsInMillis = now;
    noOfBuckets = (int) Math.ceil((now - startOfBucketsInMillis) / (bucketSpanInMillis * 1.0));
    buckets = (TimeBucket<T>[]) Array.newInstance(TimeBucket.class, noOfBuckets);
  }

  public void setup()
  {
    if(buckets == null) {
      recomputeNumBuckets();
    }
    startService();
  }

  public Object getValidTuples(T tuple)
  {
    Object key = tuple.getEventKey();
    List<Long> keyBuckets = key2Buckets.get(key);
    if(keyBuckets == null) {
      return null;
    }
    List<Event> validTuples = new ArrayList<Event>();
    for(Long idx: keyBuckets) {
      if(dirtyBuckets.get(idx) != null) {
        TimeBucket tb = (TimeBucket)dirtyBuckets.get(idx);
        List<T> events = tb.get(key);
        if(events != null) {
          for(T event: events) {
            if(Math.abs(tuple.getTime() - event.getTime()) < spanTime) {
              validTuples.add(event);
            }
          }
        }
      } else {
        int bucketIdx = (int) (idx % noOfBuckets);
        TimeBucket tb = (TimeBucket)buckets[bucketIdx];
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

  private void updateBuckets(long time) {
    if(time < endOBucketsInMillis) {
      return;
    }
    int count =(int) ((time - endOBucketsInMillis)/(bucketSpanInMillis * 1.0));
    for(int i = 0; i < count ; i++) {
      TimeBucket<T> b = buckets[i];
      if(b != null) {
        dirtyBuckets.put(b.bucketKey, b);
        buckets[i] = null;
      }
    }
  }

  public void put(T tuple)
  {
    long bucketKey = getBucketKeyFor(tuple);
    newEvent(bucketKey, tuple);
  }

  public long getBucketKeyFor(T event)
  {
    long eventTime = getTime(event);
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

  protected long getTime(T event)
  {
    return  event.getTime();
  }

  private static class Lock
  {
  }

  public void newEvent(long bucketKey, T event)
  {
    int bucketIdx = (int) (bucketKey % noOfBuckets);

    TimeBucket<T> bucket = buckets[bucketIdx];

    if (bucket == null || bucket.bucketKey != bucketKey) {
      if(bucket != null) {
        dirtyBuckets.put(bucket.bucketKey, bucket);
      }
      bucket = createBucket(bucketKey);
      buckets[bucketIdx] = bucket;
    }

    Object key = bucket.getEventKey(event);
    List<Long> keyBuckets = key2Buckets.get(key);
    if(keyBuckets == null) {
      key2Buckets.put(key, Lists.newArrayList(bucketKey));
    } else {
      key2Buckets.get(key).add(bucketKey);
    }
    bucket.addNewEvent(bucket.getEventKey(event), event);
  }

  public void startService()
  {
    bucketSlidingTimer = new Timer();
    endOBucketsInMillis = expiryTime + (noOfBuckets * bucketSpanInMillis);
    logger.debug("bucket properties {}, {}", spanTime, bucketSpanInMillis);
    logger.debug("bucket time params: start {}, end {}", startOfBucketsInMillis, endOBucketsInMillis);

    bucketSlidingTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        long time = 0;
        updateBuckets(endOBucketsInMillis + bucketSpanInMillis);
        synchronized (lock) {
          time = (expiryTime += bucketSpanInMillis);
          endOBucketsInMillis += bucketSpanInMillis;
        }
        deleteExpiredBuckets(time);
      }

    }, bucketSpanInMillis, bucketSpanInMillis);
  }

  void deleteExpiredBuckets(long time) {
    Iterator<Long> iterator = dirtyBuckets.keySet().iterator();
    for (; iterator.hasNext(); ) {
      long key = iterator.next();
      TimeBucket t = (TimeBucket)dirtyBuckets.get(key);
      if(startOfBucketsInMillis + (t.bucketKey * noOfBuckets) < time) {
        deleteBucket(t);
        iterator.remove();
      }
    }

  }

  public List<T> getUnmatchedEvents()
  {
    List<T> copyEvents = new ArrayList<T>(unmatchedEvents);
    unmatchedEvents.clear();
    return copyEvents;
  }

  private void deleteBucket(TimeBucket bucket) {
    if(bucket == null) {
      return;
    }
    Map<Object, List<T>> writtens = bucket.getEvents();
    if(writtens == null) {
      return;
    }
    for(Map.Entry<Object, List<T>> e: writtens.entrySet()) {
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

  protected TimeBucket<T> createBucket(long bucketKey)
  {
    return new TimeBucket<T>(bucketKey);
  }

  public void setSpanTime(long spanTime)
  {
    this.spanTime = spanTime;
  }

  public void setBucketSpanInMillis(int bucketSpanInMillis)
  {
    this.bucketSpanInMillis = bucketSpanInMillis;
  }

  public void shutdown()
  {
    bucketSlidingTimer.cancel();
  }

  public void setOuter(boolean isOuter)
  {
    this.isOuter = isOuter;
  }
}
