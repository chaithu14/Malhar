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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class for TimeBased Store.
 */
public class InMemoryStore extends TimeBasedStore<TimeEvent> implements BackupStore
{
  private static transient final Logger logger = LoggerFactory.getLogger(InMemoryStore.class);
  public InMemoryStore()
  {
  }

  public InMemoryStore(long spanTimeInMillis, int bucketSpanInMillis, String basePath)
  {
    super();
    setBucketRoot(basePath);
    setSpanTimeInMillis(spanTimeInMillis);
    setBucketSpanInMillis((int)(spanTimeInMillis > (long)bucketSpanInMillis ? bucketSpanInMillis : spanTimeInMillis));
  }

  public void setup()
  {
    super.setup();
  }

  @Override public void committed(long windowId)
  {
    super.committed(windowId);
  }

  @Override public void checkpointed(long windowId)
  {
    super.checkpointed(windowId);
  }

  @Override public void endWindow()
  {
    super.endWindow();
  }

  public void beginWindow(long window) {
    super.beginWindow(window);
  }

  public void shutdown()
  {
    super.shutdown();
  }

  @Override public Object getUnMatchedTuples()
  {
    return super.getUnmatchedEvents();
  }

  @Override public void isOuterJoin(Boolean isOuter)
  {  }

  @Override public Boolean put(byte[] tuple, long time, byte[] keyBytes)
  {
    return super.put(tuple, time, keyBytes);
  }

  public void setSpanTimeInMillis(long spanTimeInMillis)
  {
    super.setSpanTimeInMillis(spanTimeInMillis);
  }

  public void setBucketSpanInMillis(int bucketSpanInMillis)
  {
    super.setBucketSpanInMillis(bucketSpanInMillis);
  }
}
