package com.datatorrent.contrib.join;

import com.datatorrent.lib.bucket.*;
import com.datatorrent.lib.bucket.TimeBasedStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryStore<T extends Event & Bucketable> extends BackupStore
{
  private static transient final Logger logger = LoggerFactory.getLogger(InMemoryStore.class);

  com.datatorrent.lib.bucket.TimeBasedStore store;

  public InMemoryStore(String keyField, long spanTime, int bucketSpanInMillis)
  {
    super(keyField);
    store = new TimeBasedStore();
    store.setSpanTime(spanTime);
    store.setBucketSpanInMillis(bucketSpanInMillis);
  }

  public void setup()
  {
    store.setup();
  }

  public void shutdown()
  {
    store.shutdown();
  }

  @Override public Object getValidTuples(Object key, Object tuple)
  {
    return store.getValidTuples(key, (Event)tuple);
  }

  @Override public void put(Object tuple)
  {
    store.put((Event)tuple);
  }

  public void setSpanTime(long spanTime)
  {
    store.setSpanTime(spanTime);
  }

  public void setBucketSpanInMillis(int bucketSpanInMillis)
  {
    store.setBucketSpanInMillis(bucketSpanInMillis);
  }
}
