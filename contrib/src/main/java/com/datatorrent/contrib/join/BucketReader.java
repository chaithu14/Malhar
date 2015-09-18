package com.datatorrent.contrib.join;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

public class BucketReader
{
  public final TreeMap<byte[], DTFileReader> files;
  protected BucketReader(Comparator<byte[]> cmp)
  {
    files = new TreeMap<byte[], DTFileReader>(cmp);
  }

  @SuppressWarnings("unused")
  private BucketReader()
  {
    // for serialization only
    files = null;
  }

  public void close() throws IOException
  {
    for(Map.Entry<byte[], DTFileReader> e : files.entrySet()) {
      e.getValue().close();
    }
    files.clear();
  }

}
