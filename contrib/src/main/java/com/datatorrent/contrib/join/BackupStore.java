package com.datatorrent.contrib.join;

import org.slf4j.LoggerFactory;

public abstract class BackupStore
{
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(BackupStore.class);

  protected String keyField;

  BackupStore(String keyField)
  {
    this.keyField = keyField;
  }
  public void setup()
  {

  }

  public abstract Object getValidTuples(Object key, Object tuple);

  public void committed(long windowId)
  {

  }

  public void checkpointed(long windowId)
  {

  }

  public abstract void put(Object tuple);

  public abstract void shutdown();
}
