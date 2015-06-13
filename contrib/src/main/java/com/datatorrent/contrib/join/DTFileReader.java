package com.datatorrent.contrib.join;
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

import com.datatorrent.common.util.Slice;
import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.file.tfile.DTFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TFileReader
 *
 * @since 2.0.0
 */
public class DTFileReader implements Closeable
{
  private static transient final Logger logger = LoggerFactory.getLogger(DTFileReader.class);
  private final DTFile.Reader reader;
  private final DTFile.Reader.Scanner scanner;
  private final FSDataInputStream fsdis;
  private boolean closed = false;
  private String path;

  public DTFileReader(FSDataInputStream fsdis, long fileLength, Configuration conf, String path) throws IOException
  {
    logger.info("---Chunk Size: {}, OutSTream: {}", conf.get("tfile.io.chunk.size"), conf.get("tfile.fs.output.buffer.size"));
    logger.info("+++Chunk Size: {}, OutSTream: {}", fsdis);
    this.fsdis = fsdis;
    reader = new DTFile.Reader(fsdis, fileLength, conf);
    scanner = reader.createScanner();
    this.path = path;
  }

  /**
   * Unlike the TFile.Reader.close method this will close the wrapped InputStream.
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException
  {
    closed = true;
    scanner.close();
    reader.close();
    fsdis.close();
  }

  public void readFully(TreeMap<Slice, byte[]> data) throws IOException
  {
    scanner.rewind();
    for (; !scanner.atEnd(); scanner.advance()) {
      DTFile.Reader.Scanner.Entry en = scanner.entry();
      int klen = en.getKeyLength();
      int vlen = en.getValueLength();
      byte[] key = new byte[klen];
      byte[] value = new byte[vlen];
      en.getKey(key);
      en.getValue(value);
      data.put(new Slice(key, 0, key.length), value);
    }

  }

  public TreeMap<byte[], byte[]> readFully() throws IOException
  {
    logger.info("ReadFull start");
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
    scanner.rewind();
    for (; !scanner.atEnd(); scanner.advance()) {
      DTFile.Reader.Scanner.Entry en = scanner.entry();
      int klen = en.getKeyLength();
      int vlen = en.getValueLength();
      byte[] key = new byte[klen];
      byte[] value = new byte[vlen];
      en.getKey(key);
      en.getValue(value);
      data.put(key, value);
    }
    logger.info("ReadFull end");
    scanner.rewind();
    return data;
  }

  public void reset() throws IOException
  {
    scanner.rewind();
  }

  public boolean seek(Slice key) throws IOException
  {
    try {
      return scanner.seekTo(key.buffer, key.offset, key.length);
    } catch (NullPointerException ex) {
      if (closed)
        throw new IOException("Stream was closed");
      else
        throw ex;
    }
  }

  public boolean get(byte[] keyBytes, byte[] value) throws IOException
  {
    if (scanner.atEnd()) return false;
    if(scanner.seekTo(keyBytes, 0, keyBytes.length)) {
      DTFile.Reader.Scanner.Entry en = scanner.entry();
      byte[] rkey = new byte[en.getKeyLength()];
      value = new byte[en.getValueLength()];
      en.getKey(rkey);
      en.getValue(value);
      scanner.advance();
      return true;
    }
    return false;
  }


  public byte[] get(byte[] keyBytes) throws IOException
  {
    if (scanner.atEnd()) return null;
    if(scanner.seekTo(keyBytes, 0, keyBytes.length)) {
      DTFile.Reader.Scanner.Entry en = scanner.entry();
      byte[] rkey = new byte[en.getKeyLength()];
      byte[] value = new byte[en.getValueLength()];
      en.getKey(rkey);
      en.getValue(value);
      //scanner.advance();
      return value;
    }
    return null;
  }

  public boolean next(Slice key, Slice value) throws IOException
  {
    if (scanner.atEnd()) return false;
    DTFile.Reader.Scanner.Entry en = scanner.entry();
    byte[] rkey = new byte[en.getKeyLength()];
    byte[] rval = new byte[en.getValueLength()];
    en.getKey(rkey);
    en.getValue(rval);

    key.buffer = rkey;
    key.offset = 0;
    key.length = en.getKeyLength();

    value.buffer = rval;
    value.offset = 0;
    value.length = en.getValueLength();

    scanner.advance();
    return true;
  }

  public String getPath()
  {
    return path;
  }
}