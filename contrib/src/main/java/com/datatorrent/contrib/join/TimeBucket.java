package com.datatorrent.contrib.join;

import com.datatorrent.lib.bucket.Bucketable;
import com.google.common.collect.Lists;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeBucket<T extends Bucketable>
{
  private static transient final Logger logger = LoggerFactory.getLogger(TimeBucket.class);
  private Map<Object, List<T>> unwrittenEvents;
  private Map<Object, List<T>> writtenEvents;
  private transient BloomFilter bloomFilter;
  private boolean isDataOnDiskLoaded;
  public long bucketKey;

  protected TimeBucket()
  {

  }
  protected TimeBucket(long bucketKey)
  {
    isDataOnDiskLoaded = false;
    this.bucketKey = bucketKey;
    bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), 300000, 0.001);
  }

  public void transferEvents()
  {
    synchronized (this) {
      writtenEvents = unwrittenEvents;
      unwrittenEvents = null;
    }
  }

  Map<Object, List<T>> getWrittenEvents()
  {
    return writtenEvents;
  }

  protected Object getEventKey(T event)
  {
    return event.getEventKey();
  }

  void transferDataFromMemoryToStore()
  {
    System.out.println("===== TransferDataFromMemoryToStore ====== " + bucketKey);
    writtenEvents = null;
    isDataOnDiskLoaded = true;
  }

  public boolean isDataOnDiskLoaded()
  {
    return isDataOnDiskLoaded;
  }
  void addNewEvent(Object eventKey, T event)
  {
    synchronized (this) {
      if (unwrittenEvents == null) {
        unwrittenEvents = new HashMap<Object, List<T>>();
      }
      List<T> listEvents = unwrittenEvents.get(eventKey);
      if(listEvents == null) {
        unwrittenEvents.put(eventKey, Lists.newArrayList(event));
      } else {
        unwrittenEvents.get(eventKey).add(event);
      }
    }
    bloomFilter.put(eventKey.toString().getBytes());
    //bloomFilter.add(eventKey);
  }

  public Map<Object, List<T>> getEvents() {
    return unwrittenEvents; }

  public List<T> get(Object key) {
    if(unwrittenEvents == null && writtenEvents == null) {
      return null;
    }
    List<T> value = null;
    if(unwrittenEvents != null)
      value = unwrittenEvents.get(key);
    if(writtenEvents != null) {
      if(value != null && writtenEvents.get(key) != null) {
        value.addAll(writtenEvents.get(key));
      } else if(value == null) {
        value = writtenEvents.get(key);
      }
    }
    return value;
    //return unwrittenEvents.get(key);
  }

  public boolean contains(Object key)
  {
    //return bloomFilter.contains(key);
    return bloomFilter.mightContain(key.toString().getBytes());
  }
}

