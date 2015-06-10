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
import com.google.common.collect.Maps;
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
  public final long bucketKey;

  public Bucket() {
      bucketKey = -1L;
  }

  protected Bucket(long bucketKey)
  {
    this.bucketKey = bucketKey;
  }

  protected Object getEventKey(T event)
  {
    return event.getEventKey();
  }

  /**
   * Add the given event into the unwritternEvents map
   * @param eventKey
   * @param event
   */
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
