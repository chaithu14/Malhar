package com.datatorrent.contrib.join;

import com.datatorrent.lib.bucket.*;
import com.datatorrent.lib.bucket.TimeBasedStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryStore<T extends Event & Bucketable> extends TimeBasedStore implements BackupStore
{
  private static transient final Logger logger = LoggerFactory.getLogger(InMemoryStore.class);

  public InMemoryStore(long spanTime, int bucketSpanInMillis)
  {
    super();
    setSpanTime(spanTime);
    setBucketSpanInMillis((int)(spanTime > (long)bucketSpanInMillis ? bucketSpanInMillis : spanTime));
  }

  public void setup()
  {
    super.setup();
  }

  public void shutdown()
  {
    super.shutdown();
  }

  @Override public void endWindow()
  {

  }

  @Override public Object getValidTuples(Object tuple)
  {
    return super.getValidTuples((Event) tuple);
  }

  @Override public void committed(long windowId)
  {

  }

  @Override public void checkpointed(long windowId)
  {

  }

  @Override public void put(Object tuple)
  {
    super.put((Event)tuple);
  }

  public void setSpanTime(long spanTime)
  {
    super.setSpanTime(spanTime);
  }

  public void setBucketSpanInMillis(int bucketSpanInMillis)
  {
    super.setBucketSpanInMillis(bucketSpanInMillis);
  }
}
