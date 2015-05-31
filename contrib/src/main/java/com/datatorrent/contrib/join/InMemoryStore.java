package com.datatorrent.contrib.join;

import com.datatorrent.api.Context;
import com.datatorrent.lib.bucket.Bucketable;
import com.datatorrent.lib.bucket.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryStore<T extends Event & Bucketable> extends TimeBasedStore implements BackupStore
{
  private static transient final Logger logger = LoggerFactory.getLogger(InMemoryStore.class);

  public InMemoryStore()
  {

  }
  public InMemoryStore(long spanTime, int bucketSpanInMillis, String basePath)
  {
    super();
    setBucketRoot(basePath);
    setSpanTime(spanTime);
    setBucketSpanInMillis((int)(spanTime > (long)bucketSpanInMillis ? bucketSpanInMillis : spanTime));
  }

  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
  }

  public void shutdown()
  {
    super.shutdown();
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
