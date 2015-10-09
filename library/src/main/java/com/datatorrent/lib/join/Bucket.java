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
package com.datatorrent.lib.join;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * <p>
 * This is the base implementation of bucket which contains all the events which belong to the same bucket.
 * </p>
 *
 * @param <T> type of bucket events
 * @since 2.2.0
 */
public class Bucket<T extends TimeEvent>
{
  public final long bucketKey;
  protected Map<Object, List<T>> unwrittenEvents;

  public Bucket()
  {
    bucketKey = -1L;
  }

  protected Bucket(long bucketKey)
  {
    this.bucketKey = bucketKey;
  }

  /**
   * Add the given event into the unwritternEvents map
   *
   * @param eventKey
   * @param event
   */
  protected void addNewEvent(Object eventKey, T event)
  {
    if (unwrittenEvents == null) {
      unwrittenEvents = Maps.newHashMap();
    }
    List<T> listEvents = unwrittenEvents.get(eventKey);
    if (listEvents == null) {
      unwrittenEvents.put(eventKey, Lists.newArrayList(event));
    } else {
      listEvents.add(event);
    }
  }

  /**
   * Return the unwritten events in the bucket
   *
   * @return
   */
  public Map<Object, List<T>> getEvents()
  {
    return unwrittenEvents;
  }

  /**
   * Return the list of events for the given key
   *
   * @param key
   * @return
   */
  public List<T> get(Object key)
  {
    return unwrittenEvents.get(key);
  }
}
