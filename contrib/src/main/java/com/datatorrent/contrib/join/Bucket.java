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

import com.datatorrent.lib.bucket.Bucketable;
import com.google.common.collect.Lists;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * This is the base implementation of bucket which contains all the events which belong to the same bucket.
 * </p>
 *
 * @param <T> type of bucket events
 * @since 2.2.0
 */
public class Bucket<T extends Bucketable>
{
  private Map<Object, List<T>> unwrittenEvents;
  private Map<Object, List<T>> writtenEvents;
  private transient BloomFilter bloomFilter;
  private boolean isDataOnDiskLoaded;

  public final long bucketKey;

  public Bucket() {
      bucketKey = -1L;
  }

  protected Bucket(long bucketKey)
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

  public Map<Object, List<T>> getWrittenEvents()
  {
    return writtenEvents;
  }

  protected Object getEventKey(T event)
  {
    return event.getEventKey();
  }

  void transferDataFromMemoryToStore()
  {
    writtenEvents = null;
    isDataOnDiskLoaded = true;
  }

  public boolean isDataOnDiskLoaded()
  {
    return isDataOnDiskLoaded;
  }

  /**
   * Add the given event into the unwritternEvents map
   * @param eventKey
   * @param event
   */
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
        listEvents.add(event);
      }
    }
    bloomFilter.put(eventKey.toString().getBytes());

  }

  public Map<Object, List<T>> getEvents() { return unwrittenEvents; }

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
  }

  public boolean contains(Object key)
  {
    if(bloomFilter == null || key == null)
      return false;
    return bloomFilter.mightContain(key.toString().getBytes());
  }

  public void clear()
  {
    if(writtenEvents != null)
      writtenEvents.clear();
    if(unwrittenEvents != null)
      unwrittenEvents.clear();
    if(bloomFilter != null)
      bloomFilter=null;
  }
}
