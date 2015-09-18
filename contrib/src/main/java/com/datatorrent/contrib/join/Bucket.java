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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * <p>
 * This is the base implementation of bucket which contains all the events which belong to the same bucket.
 * </p>
 *
 * @param <> type of bucket events
 * @since 2.2.0
 */
public class Bucket
{
  private static transient final Logger logger = LoggerFactory.getLogger(Bucket.class);
  private Map<ByteBuffer, List<byte[]>> unwrittenEvents;
  private Map<ByteBuffer, List<byte[]>> writtenEvents;
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
    bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), 1000000, 0.001);
  }

  protected Bucket(long bucketKey, long maxSize)
  {
    this(bucketKey);
  }
  public void transferEvents()
  {
    writtenEvents = unwrittenEvents;
    unwrittenEvents = null;
  }

  public Map<ByteBuffer, List<byte[]>> getWrittenEvents()
  {
    return writtenEvents;
  }

  /*protected Object getEventKey(byte[] event)
  {
    return event.getEventKey();
  }*/

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
  void addNewEvent(byte[] eventKey, byte[] event)
  {
    if (unwrittenEvents == null) {
      unwrittenEvents = new HashMap<ByteBuffer, List<byte[]>>();
    }
    List<byte[]> listEvents = unwrittenEvents.get(ByteBuffer.wrap(eventKey));

    if(listEvents == null) {
      unwrittenEvents.put(ByteBuffer.wrap(eventKey), Lists.newArrayList(event));
      bloomFilter.put(eventKey);
    } else {
      listEvents.add(event);
    }
  }

  public Map<ByteBuffer, List<byte[]>> getEvents() { return unwrittenEvents; }

  public List<byte[]> get(byte[] key) {
    if(unwrittenEvents == null && writtenEvents == null) {
      return null;
    }
    ByteBuffer keyBuffer = ByteBuffer.wrap(key);
    List<byte[]> value = null;
    if(unwrittenEvents != null)
      value = unwrittenEvents.get(keyBuffer);
    if(writtenEvents != null) {
      if(value != null && writtenEvents.get(keyBuffer) != null) {
        value.addAll(writtenEvents.get(keyBuffer));
      } else if(value == null) {
        value = writtenEvents.get(keyBuffer);
      }
    }
    return value;
  }

  public boolean contains(byte[] key)
  {
    if(bloomFilter == null || key == null)
      return false;
    return bloomFilter.mightContain(key);
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
