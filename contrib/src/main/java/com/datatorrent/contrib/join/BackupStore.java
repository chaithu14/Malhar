package com.datatorrent.contrib.join;

import com.datatorrent.api.Context;

public interface BackupStore
{
  void setup(Context.OperatorContext context);

  Object getValidTuples(Object tuple);

  void committed(long windowId);

  void checkpointed(long windowId);

  void put(Object tuple);

  void shutdown();

  void endWindow(long windowId);
}
