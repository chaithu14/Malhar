/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.hds;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import com.datatorrent.lib.hds.HDS.TimeSeriesDataKey;
import com.datatorrent.lib.util.KeyValPair;

/**
 *
 */
public class HDSTest
{
  public static class MyDataKey implements TimeSeriesDataKey
  {
    public String bucketKey;
    public long timestamp;
    public String data;

    @Override
    public String getBucketKey()
    {
      return bucketKey;
    }

    @Override
    public long getTime()
    {
      return timestamp;
    }

    @Override
    public int hashCode()
    {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((bucketKey == null) ? 0 : bucketKey.hashCode());
      result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
      return result;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      MyDataKey other = (MyDataKey) obj;
      if (bucketKey == null) {
        if (other.bucketKey != null)
          return false;
      } else if (!bucketKey.equals(other.bucketKey))
        return false;
      if (timestamp != other.timestamp)
        return false;
      return true;
    }

  }

  @Test
  public void test() throws Exception
  {
    File file = new File("target/hds");
    FileUtils.deleteDirectory(file);

    FileSystem fs = FileSystem.getLocal(new Configuration(false)).getRawFileSystem();
    BucketFileSystem bfs = new FSBucketFileSystem(fs, file.getAbsolutePath());

    HDSPrototype<MyDataKey, String> hds = new HDSPrototype<HDSTest.MyDataKey, String>();
    hds.bfs = bfs;
    hds.timeBucketUnit = TimeUnit.MILLISECONDS;
    hds.timeBucketSize = 10;
    hds.maxSize = 500;
    hds.init();

    MyDataKey key1 = new MyDataKey();
    key1.bucketKey = "bucket1";
    key1.timestamp = 1;
    key1.data = "data01bucket1";
    KeyValPair<MyDataKey, String> entry1 = new KeyValPair<HDSTest.MyDataKey, String>(key1, key1.data);

    hds.put(entry1);

    File bucket1Dir = new File(file, "bucket1");
    File bucket1DupsFile = new File(bucket1Dir, FSBucketFileSystem.DUPLICATES);
    Assert.assertTrue("exists " + bucket1Dir, bucket1Dir.exists() && bucket1Dir.isDirectory());
    Assert.assertFalse("exists " + bucket1DupsFile, bucket1DupsFile.exists());

    RegexFileFilter ff = new RegexFileFilter("bucket.*");
    String[] files = bucket1Dir.list(ff);
    Assert.assertEquals("" + Arrays.asList(files), 1, files.length);

    hds.put(entry1); // duplicate key
    files = bucket1Dir.list(ff);
    Assert.assertEquals("" + Arrays.asList(files), 1, files.length);
    Assert.assertTrue("exists " + bucket1DupsFile, bucket1DupsFile.exists() && bucket1DupsFile.isFile());

    MyDataKey key12 = new MyDataKey();
    key12.bucketKey = "bucket1";
    key12.timestamp = 2;
    key12.data = "data02bucket1";
    KeyValPair<MyDataKey, String> entry12 = new KeyValPair<HDSTest.MyDataKey, String>(key12, key12.data);

    hds.put(entry12); // same time bucket key
    files = bucket1Dir.list(ff);
    Assert.assertEquals("" + Arrays.asList(files), 1, files.length);
    Assert.assertTrue("exists " + bucket1DupsFile, bucket1DupsFile.exists() && bucket1DupsFile.isFile());


    MyDataKey key2 = new MyDataKey();
    key2.bucketKey = "bucket1";
    key2.timestamp = 11;
    key1.data = "data11bucket1";

    KeyValPair<MyDataKey, String> entry2 = new KeyValPair<HDSTest.MyDataKey, String>(key2, key2.data);
    hds.put(entry2);
    files = bucket1Dir.list(ff);
    Assert.assertEquals("" + Arrays.asList(files), 2, files.length);

  }

}