package com.datatorrent.contrib.join;

import org.slf4j.LoggerFactory;

public abstract class BackupStore
{
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(BackupStore.class);

  protected String keyField;

  protected AbstractJoinOperator op;

  BackupStore(String keyField, AbstractJoinOperator op)
  {
    this.keyField = keyField;
    this.op = op;
  }
  public void setup()
  {

  }

  public Object getKey(Object tuple)
  {
    return op.getValue(keyField, tuple);
  }


  public abstract Object getValidTuples(Object key, Object tuple);

  public void committed(long windowId)
  {

  }

  public void checkpointed(long windowId)
  {

  }

  public abstract void put(Object tuple);
}
