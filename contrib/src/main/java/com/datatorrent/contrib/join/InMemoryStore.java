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

  public InMemoryStore(long spanTimeInMillis, int bucketSpanInMillis)
  {
    super();
    setSpanTimeInMillis(spanTimeInMillis);
    setBucketSpanInMillis(bucketSpanInMillis);
  }

  public InMemoryStore(long spanTimeInMillis, int bucketSpanInMillis, String bucketPath)
  {
    super();
    setSpanTimeInMillis(spanTimeInMillis);
    setBucketSpanInMillis(bucketSpanInMillis);
    setBucketRoot(bucketPath);
  }

  public void setup()
  {
    super.setup();
  }

  @Override public void committed(long windowId)
  {

  }

  @Override public void checkpointed(long windowId)
  {

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
  {
    super.isOuterJoin(isOuter);
  }

  @Override public Object getValidTuples(Object tuple)
  {
    return super.getValidTuples((TimeEvent) tuple);
  }

  @Override public Boolean put(Object tuple)
  {
    return super.put((TimeEvent)tuple);
  }


}
