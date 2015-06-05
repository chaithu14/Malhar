package com.datatorrent.contrib.join;

import com.datatorrent.common.util.Slice;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;


public class HFileReader implements Closeable
{
  private final HFile.Reader reader;
  private final HFileScanner scanner;
  private final String path;
  private boolean closed = false;

  public HFileReader(long fileLength, Configuration conf, String path) throws IOException
  {
    this.path = path;
    Path dataFilePath = new Path(path);
    CacheConfig cacheConf = new CacheConfig(conf);
    FileSystem fs = FileSystem.newInstance(dataFilePath.toUri(), conf);
    reader = HFile.createReader(fs, dataFilePath, cacheConf, conf);
    scanner = reader.getScanner(true, true, false);
  }

  /**
   * Unlike the TFile.Reader.close method this will close the wrapped InputStream.
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException
  {
    closed = true;
    //scanner.close();
    reader.close();
    //fsdis.close();
  }

  public TreeMap<byte[], byte[]> readFully() throws IOException
  {
    scanner.seekTo();
    TreeMap<byte[], byte[]> data = new TreeMap<byte[], byte[]>(new Comparator<byte[]>()
    {
      @Override public int compare(byte[] bytes, byte[] bytes2)
      {
        int end1 = bytes.length;
        int end2 = bytes2.length;
        for (int i = 0, j = 0; i < end1 && j < end2; i++, j++) {
          int a = (bytes[i] & 0xff);
          int b = (bytes2[j] & 0xff);
          if (a != b) {
            return a - b;
          }
        }
        return end1 - end2;
      }
    });
    KeyValue kv;
    do {
      kv = scanner.getKeyValue();
      Slice key = new Slice(kv.getRowArray(), kv.getKeyOffset(), kv.getKeyLength());
      data.put(key.buffer, Arrays.copyOfRange(kv.getRowArray(), kv.getValueOffset(), kv.getValueOffset() + kv.getValueLength()));
    } while (scanner.next());

    return data;
  }

  public boolean get(byte[] keyBytes, byte[] value) throws IOException
  {
    if(scanner.seekTo(keyBytes) == 0)
    {
      KeyValue kv = scanner.getKeyValue();
      if (kv == null) {
        // cursor is already at the end
        return false;
      }
      value = kv.getRowArray();
      scanner.next();
      return true;
    }
    return false;
  }

  public String getPath()
  {
    return path;
  }
}

