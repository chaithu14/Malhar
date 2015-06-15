package com.datatorrent.contrib.join;

import com.datatorrent.lib.bucket.Bucketable;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeBucket<T extends Bucketable>
{
  private static transient final Logger logger = LoggerFactory.getLogger(TimeBucket.class);
  private Map<Object, List<T>> unwrittenEvents;
  private Map<Object, List<T>> allUnwrittenEvents;
  private Map<Object, List<T>> writtenEvents;
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
      unwrittenEvents = null;
  }

  Map<Object, List<T>> getWrittenEvents()
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
    if (unwrittenEvents == null) {
      unwrittenEvents = new HashMap<Object, List<T>>();
    }
    if(allUnwrittenEvents == null) {
      allUnwrittenEvents = new HashMap<Object, List<T>>();
    }
    List<T> events = unwrittenEvents.get(eventKey);
    List<T> allEvents = allUnwrittenEvents.get(eventKey);
    if(events == null) {
      unwrittenEvents.put(eventKey, Lists.newArrayList(event));
      if(allEvents == null) {
        allUnwrittenEvents.put(eventKey, Lists.newArrayList(event));
      } else {
        allEvents.add(event);
      }
    } else {
      events.add(event);
      allEvents.add(event);
    }



    //bloomFilter.put(eventKey.toString().getBytes());
    //bloomFilter.add(eventKey);
  }

  public Map<Object, List<T>> getEvents() {
    return unwrittenEvents; }

  public List<T> get(Object key) {
    if(allUnwrittenEvents == null && writtenEvents == null) {
      return null;
    }
    List<T> value = null;
    if(allUnwrittenEvents != null)
      value = (List<T>) allUnwrittenEvents.get(key);
    if(writtenEvents != null) {
      if(value != null && writtenEvents.get(key) != null) {
        value.addAll(writtenEvents.get(key));
      } else if(value == null) {
        value = (List<T>) writtenEvents.get(key);
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

