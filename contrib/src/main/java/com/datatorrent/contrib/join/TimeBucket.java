package com.datatorrent.contrib.join;

import com.datatorrent.lib.bucket.AbstractBucket;
import com.datatorrent.lib.bucket.Bucketable;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeBucket<T extends Bucketable> extends AbstractBucket<T>
{
  private static transient final Logger logger = LoggerFactory.getLogger(TimeBucket.class);
  private Map<Object, List<T>> unwrittenEvents;
  private boolean isDataOnDiskLoaded;

  protected TimeBucket(long bucketKey)
  {
    super(bucketKey);
    isDataOnDiskLoaded = false;
  }

  @Override
  protected Object getEventKey(T event)
  {
    return event.getEventKey();
  }

  void transferDataFromMemoryToStore()
  {
    System.out.println("===================================");
    unwrittenEvents = null;
    isDataOnDiskLoaded = true;
  }

  public boolean isDataOnDiskLoaded()
  {
    return isDataOnDiskLoaded;
  }
  void addNewEvent(Object eventKey, T event)
  {
    if (unwrittenEvents == null) {
      unwrittenEvents = new TreeMap<Object, List<T>>();
    }
    List<T> listEvents = unwrittenEvents.get(eventKey);
    if(listEvents == null) {
      unwrittenEvents.put(eventKey, Lists.newArrayList(event));
    } else {
      unwrittenEvents.get(eventKey).add(event);
    }
  }

  public Map<Object, List<T>> getEvents() {
    return unwrittenEvents; }

  public List<T> get(Object key) {
    if(unwrittenEvents == null) {
      return null;
    }
    return unwrittenEvents.get(key);
  }
}

