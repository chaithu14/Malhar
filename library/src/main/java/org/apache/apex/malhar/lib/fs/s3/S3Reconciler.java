/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.apex.malhar.lib.fs.s3;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.esotericsoftware.kryo.NotNull;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.fs.AbstractReconciler;

public class S3Reconciler extends AbstractReconciler<S3Reconciler.OutputMetaData, S3Reconciler.OutputMetaData>
{
  private static final Logger logger = LoggerFactory.getLogger(S3Reconciler.class);
  private static final String TMP_EXTENSION = ".tmp";
  @NotNull
  private String accessKey;
  @NotNull
  private String secretKey;
  @NotNull
  private String bucketName;
  @NotNull
  private String directoryName;
  protected transient AmazonS3 s3client;
  protected transient FileSystem fs;
  protected transient String filePath;

  @Override
  public void setup(Context.OperatorContext context)
  {
    s3client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
    filePath = context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + S3CompactionOperator.recoveryPath;
    try {
      fs = FileSystem.newInstance(new Path(filePath).toUri(), new Configuration());
    } catch (IOException e) {
      logger.error("Unable to create FileSystem: {}", e.getMessage());
    }
    super.setup(context);
  }

  @Override
  protected void processTuple(S3Reconciler.OutputMetaData outputMetaData)
  {
    enqueueForProcessing(outputMetaData);
  }

  @Override
  protected void processCommittedData(S3Reconciler.OutputMetaData outputMetaData)
  {
    try {
      FSDataInputStream fsinput = fs.open(new Path(outputMetaData.getPath()));
      ObjectMetadata omd = new ObjectMetadata();
      omd.setContentLength(outputMetaData.getSize());
      String keyName = directoryName + Path.SEPARATOR + outputMetaData.getFileName();
      s3client.putObject(new PutObjectRequest(bucketName, keyName, fsinput, omd));
    } catch (IOException e) {
      logger.error("Unable to create Stream: {}", e.getMessage());
    }
  }

  @Override
  public void endWindow()
  {
    logger.info("in endWindow()");
    while (doneTuples.peek() != null) {
      S3Reconciler.OutputMetaData metaData = doneTuples.poll();
      logger.info("found metaData = {}", metaData);
      removeProcessedTuple(metaData);
    }
  }

  public void removeProcessedTuple(S3Reconciler.OutputMetaData metaData)
  {
    committedTuples.remove(metaData);
    try {
      Path dest = new Path(metaData.getPath());
      //Deleting the intermediate files and when writing to tmp files
      // there can be vagrant tmp files which we have to clean
      FileStatus[] statuses = fs.listStatus(dest.getParent());
      logger.info("found statuses = {}", statuses);

      for (FileStatus status : statuses) {
        String statusName = status.getPath().getName();
        logger.info("statusName = {}", statusName);
        if (statusName.endsWith(TMP_EXTENSION) && statusName.startsWith(metaData.getFileName())) {
          //a tmp file has tmp extension always preceded by timestamp
          String actualFileName = statusName.substring(0,
              statusName.lastIndexOf('.', statusName.lastIndexOf('.') - 1));
          logger.info("actualFileName = {}", actualFileName);
          if (metaData.getFileName().equals(actualFileName)) {
            logger.info("deleting stray file {}", statusName);
            fs.delete(status.getPath(), true);
          }
        } else if (statusName.equals(metaData.getFileName())) {
          logger.info("deleting s3-compaction file {}", statusName);
          fs.delete(status.getPath(), true);
        }
      }
    } catch (IOException e) {
      logger.error("Unable to Delete a file: {}", metaData.getFileName());
    }
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

  public String getDirectoryName()
  {
    return directoryName;
  }

  public void setDirectoryName(String directoryName)
  {
    this.directoryName = directoryName;
  }

  void setS3client(AmazonS3 s3client)
  {
    this.s3client = s3client;
  }

  public static class OutputMetaData
  {
    private String path;
    private String fileName;
    private long size;

    public OutputMetaData()
    {

    }

    public OutputMetaData(String path, String fileName, long size)
    {
      this.path = path;
      this.fileName = fileName;
      this.size = size;
    }

    public String getPath()
    {
      return path;
    }

    public void setPath(String path)
    {
      this.path = path;
    }

    public String getFileName()
    {
      return fileName;
    }

    public void setFileName(String fileName)
    {
      this.fileName = fileName;
    }

    public long getSize()
    {
      return size;
    }

    public void setSize(long size)
    {
      this.size = size;
    }
  }
}
