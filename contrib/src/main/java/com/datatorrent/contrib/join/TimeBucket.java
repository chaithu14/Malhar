package com.datatorrent.contrib.join;

import com.datatorrent.lib.bucket.AbstractBucket;
import com.datatorrent.lib.bucket.Bucketable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

public class TimeBucket<T extends Bucketable> extends AbstractBucket<T>
{
  private Map<Object, List<T>> unwrittenEvents;

  public TimeBucket() {
    super();
  }

  protected TimeBucket(long bucketKey)
  {
    super(bucketKey);
  }

  @Override
  protected Object getEventKey(T event)
  {
    return event.getEventKey();
  }

  void addNewEvent(Object eventKey, T event)
  {
    if (unwrittenEvents == null) {
      unwrittenEvents = Maps.newHashMap();
    }
    List<T> listEvents = unwrittenEvents.get(eventKey);
    if(listEvents == null) {
      unwrittenEvents.put(eventKey, Lists.newArrayList(event));
    } else {
      unwrittenEvents.get(eventKey).add(event);
    }
  }

  public Map<Object, List<T>> getEvents() { return unwrittenEvents; }
  public List<T> get(Object key) {
    return unwrittenEvents.get(key);
  }
}
