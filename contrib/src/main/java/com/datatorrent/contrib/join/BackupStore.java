package com.datatorrent.contrib.join;

public interface BackupStore
{
  void setup();

  Object getValidTuples(Object tuple);

  void committed(long windowId);

  void checkpointed(long windowId);

  void put(Object tuple);

  void shutdown();

  void endWindow();

  Object getUnMatchedTuples();
}
