package com.datatorrent.contrib.join;

import com.datatorrent.lib.bucket.Bucketable;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeBucket<T extends Bucketable>
{
  private static transient final Logger logger = LoggerFactory.getLogger(TimeBucket.class);
  private Multimap<Object, T> unwrittenEvents;
  private Multimap<Object, T> allUnwrittenEvents;
  private Multimap<Object, T> writtenEvents;
  //private transient BloomFilter bloomFilter;
  private boolean isDataOnDiskLoaded;
  public long bucketKey;

  protected TimeBucket()
  {

  }
  protected TimeBucket(long bucketKey)
  {
    isDataOnDiskLoaded = false;
    this.bucketKey = bucketKey;
    //bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), 300000, 0.001);
  }

  public void transferEvents()
  {
    if(allUnwrittenEvents == null) {
      allUnwrittenEvents = ArrayListMultimap.create();
    }
      allUnwrittenEvents.putAll(unwrittenEvents);
      unwrittenEvents = null;
  }

  Multimap<Object, T> getWrittenEvents()
  {
    return writtenEvents;
  }

  protected Object getEventKey(T event)
  {
    return event.getEventKey();
  }

  public void snappshotEvents()
  {
    writtenEvents = allUnwrittenEvents;
    allUnwrittenEvents = null;
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
        unwrittenEvents = ArrayListMultimap.create();
      }
      unwrittenEvents.put(eventKey, event);
    }
    //bloomFilter.put(eventKey.toString().getBytes());
    //bloomFilter.add(eventKey);
  }

  public Multimap<Object, T> getEvents() {
    return unwrittenEvents; }

  public List<T> get(Object key) {
    if(unwrittenEvents == null && writtenEvents == null) {
      return null;
    }
    List<T> value = null;
    if(unwrittenEvents != null)
      value = (List<T>) unwrittenEvents.get(key);
    if(writtenEvents != null) {
      if(value != null && writtenEvents.get(key) != null) {
        value.addAll(writtenEvents.get(key));
      } else if(value == null) {
        value = (List<T>) writtenEvents.get(key);
      }
    }
    if(allUnwrittenEvents != null) {
      if(value != null && allUnwrittenEvents.get(key) != null) {
        value.addAll(allUnwrittenEvents.get(key));
      } else if(value == null) {
        value = (List<T>) allUnwrittenEvents.get(key);
      }
    }

    return value;
    //return unwrittenEvents.get(key);
  }

  /*public boolean contains(Object key)
  {
    //return bloomFilter.contains(key);
    return bloomFilter.mightContain(key.toString().getBytes());
  }*/
}

