package com.datatorrent.lib.io.fs;

import com.esotericsoftware.kryo.NotNull;

public class S3FileCopyOutputModule extends FSCopyOutputModule
{
  @NotNull
  private String accessKey;
  @NotNull
  private String secretKey;
  @NotNull
  private String bucketName;
  @Override
  public BlockWriter createBlockWriter()
  {
    return new S3BlockWriter();
  }

  @Override
  public Synchronizer createSynchronizer()
  {
    return new Synchronizer();
  }

  @Override
  public FileMerger createFileMerger()
  {
    S3FileMerger s3Merger = new S3FileMerger();
    s3Merger.setAccessKey(accessKey);
    s3Merger.setSecretKey(secretKey);
    s3Merger.setBucketName(bucketName);
    s3Merger.setDirectoryName(outputDirectoryPath);
    return s3Merger;
  }

  public String getAccessKey()
  {
    return accessKey;
  }

  public void setAccessKey(String accessKey)
  {
    this.accessKey = accessKey;
  }

  public String getSecretKey()
  {
    return secretKey;
  }

  public void setSecretKey(String secretKey)
  {
    this.secretKey = secretKey;
  }

  public String getBucketName()
  {
    return bucketName;
  }

  public void setBucketName(String bucketName)
  {
    this.bucketName = bucketName;
  }
}
