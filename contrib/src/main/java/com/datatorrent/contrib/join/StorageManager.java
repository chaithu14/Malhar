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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages WAL for a bucket.
 * When a tuple is added to WAL, is it immediately written to the
 * file, but not flushed, flushing happens at end of the operator
 * window during endWindow call. At end of window if WAL file size
 * have grown a beyond maxWalFileSize then current file is closed
 * and new file is created.
 *
 * The WAL usage windowId as log sequence number(LSN). When data is
 * written to data files, the committedWid saved in bucket metadata.
 *
 * The windowId upto which data is available is stored in BucketManager
 * WalMetadata and checkpointed with operator state.
 *
 * Recovery After Failure.
 *
 *   If committedWid is smaller than wal windowId.
 *   - Truncate last WAL file to known offset (recoveryEndWalOffset).
 *   - Wal metadata contains file id and recoveryEndWalOffset where committedWid ended,
 *     start reading from that location till the end of current WAL file
 *     and adds tuples back to the committed cache in store.
 *
 *   If committedWid is greater than wal windowId
 *   The data was committed to disks after last operator checkpoint. In this
 *   case recovery is not needed all data from WAL is already written to data
 *   files. We will reprocess tuples which are in between committedWid and wal windowId.
 *   This will not cause problem now, because file write is idempotent with
 *   duplicate tuples.
 *
 * @since 2.0.0
 */
public class StorageManager implements Closeable
{
  public static final String WAL_FILE_PREFIX = "_WAL-";

  public void setBucketRoot(long bucketKey)
  {
    this.bucketRoot = bucketRoot;
  }

  /*
   * If maximum number of bytes allowed to be written to file between flush,
   * default is 64K.
   */
  transient long maxUnflushedBytes = 64 * 1024;

  /* Maximum number of bytes per WAL file,
   * default is 128M */
  transient long maxWalFileSize = 2 * 1024 * 1024;

  /* The class responsible writing WAL entry to file */
  transient HDHTStorageWriter writer;

  transient private String bucketRoot;

  private boolean dirty;

  /* Last committed LSN on disk */
  private long flushedWid = -1;

  /* current active WAL file id, it is read from WAL meta on startup */
  private long walFileId = -1;

  /* Current WAL size */
  private long walSize = 0;

  @SuppressWarnings("unused")
  private StorageManager() {}

  public StorageManager(String bucketRoot) {
    this.bucketRoot = bucketRoot;
    walFileId = 0;
  }



  public void append(Object value) throws IOException
  {
    if (writer == null)
      writer = new HDHTStorageWriter(bucketRoot, WAL_FILE_PREFIX + walFileId);

    writer.append(value);
    /*long bytes = key.length + value.length + 2 * 4;
    stats.totalBytes += bytes;
    stats.totalKeys ++;*/
    dirty = true;

    if (maxUnflushedBytes > 0 && writer.getUnflushedCount() > maxUnflushedBytes)
    {
      flushWal();
    }
  }

  protected void flushWal() throws IOException
  {
    if (writer == null)
      return;
    long startTime = System.currentTimeMillis();
    writer.flush();

    stats.flushCounts++;
    stats.flushDuration += System.currentTimeMillis() - startTime;
  }

  /* batch writes, and wait till file is written */
  public void endWindow(long windowId) throws IOException
  {
    /* No tuple added in this window, no need to do anything. */
    if (!dirty)
      return;

    flushWal();

    dirty = false;
    flushedWid = windowId;
    walSize = writer.logSize();

    logger.info("writer.logSize: {} -> {}", bucketRoot, writer.logSize());
    /* Roll over log, if we have crossed the log size */
    if (maxWalFileSize > 0 && writer.logSize() > maxWalFileSize) {
      logger.info("Rolling over log {} windowid {}", writer, windowId);
      writer.close();
      walFileId++;
      writer = null;
      walSize = 0;
    }
  }

  /**
   * Remove files older than recoveryStartWalFileId.
   */
  /*public void cleanup(long recoveryStartWalFileId)
  {
    if (recoveryStartWalFileId == 0)
      return;

    recoveryStartWalFileId--;
    try {
      while (true) {
        DataInputStream in = bfs.getInputStream(bucketKey, WAL_FILE_PREFIX + recoveryStartWalFileId);
        in.close();
        logger.info("deleting WAL file {}", recoveryStartWalFileId);
        bfs.delete(bucketKey, WAL_FILE_PREFIX + recoveryStartWalFileId);
        recoveryStartWalFileId--;
      }
    } catch (FileNotFoundException ex) {
    } catch (IOException ex) {
    }
  }*/

  public long getMaxWalFileSize()
  {
    return maxWalFileSize;
  }

  public void setMaxWalFileSize(long maxWalFileSize)
  {
    this.maxWalFileSize = maxWalFileSize;
  }

  public long getMaxUnflushedBytes()
  {
    return maxUnflushedBytes;
  }

  public void setMaxUnflushedBytes(long maxUnflushedBytes)
  {
    this.maxUnflushedBytes = maxUnflushedBytes;
  }

  public long getFlushedWid() {
    return flushedWid;
  }

  @Override
  public void close() throws IOException
  {
    if (writer != null)
      writer.close();
  }

  public long getWalFileId()
  {
    return walFileId;
  }

  public long getWalSize()
  {
    return walSize;
  }

  public WalPosition getCurrentPosition() {
    return new WalPosition(walFileId, walSize);
  }

  private static transient final Logger logger = LoggerFactory.getLogger(StorageManager.class);

  /**
   * Stats related functionality
   */
  public static class WalStats
  {
    long totalBytes;
    long flushCounts;
    long flushDuration;
    public long totalKeys;
  }

  private final WalStats stats = new WalStats();

  /* Location of the WAL */
  public static class WalPosition {
    long fileId;
    long offset;

    private WalPosition() {
    }

    public WalPosition(long fileId, long offset) {
      this.fileId = fileId;
      this.offset = offset;
    }

    public WalPosition copyOf() {
      return new WalPosition(fileId, offset);
    }

    @Override public String toString()
    {
      return "WalPosition{" +
          "fileId=" + fileId +
          ", offset=" + offset +
          '}';
    }
  }

  public WalStats getCounters() {
    return stats;
  }

  public static class HDHTStorageWriter {

    FileSystem fs;
    FSDataOutputStream dataStream = null;
    Output output = null;
    static transient final String PATH_SEPARATOR = "/";
    protected transient Configuration configuration = new Configuration();

    protected Kryo kryo = new Kryo();
    HDHTStorageWriter(String path, String name)
    {
      Path dataFilePath = new Path(path + PATH_SEPARATOR + name);
      try {
        fs = FileSystem.newInstance(dataFilePath.toUri(), configuration);
        dataStream = fs.create(dataFilePath);
        output = new Output(dataStream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    }

    public void append(Object value)
    {
      kryo.writeObject(output, value);
    }

    public int getUnflushedCount()
    {
      return 0;
    }
    public long logSize()
    {
      return output.total();
    }

    public void flush()
    {
      output.flush();
    }

    public void close()
    {
      output.close();
      try {
        dataStream.close();
        fs.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    }
  }
}
