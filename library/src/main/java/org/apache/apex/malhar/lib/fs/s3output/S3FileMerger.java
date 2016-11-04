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
package org.apache.apex.malhar.lib.fs.s3output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.hadoop.classification.InterfaceStability;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;

/**
 * This operator can be used to merge the S3 blocks into a file. This operator will request for
 * S3 CompleteMultipartUploadRequest once all the blocks are uploaded using multi-part feature.
 */

@InterfaceStability.Evolving
public class S3FileMerger implements Operator, Operator.CheckpointNotificationListener
{
  private static final Logger LOG = LoggerFactory.getLogger(S3FileMerger.class);
  @NotNull
  private String bucketName;
  @NotNull
  private String accessKey;
  @NotNull
  private String secretAccessKey;
  private String endPoint;
  protected List<String> uploadedFiles = new ArrayList<>();
  private WindowDataManager windowDataManager = new FSWindowDataManager();
  @FieldSerializer.Bind(JavaSerializer.class)
  private Map<String, List<PartETag>> uploadParts = new HashMap<>();
  private Map<String, S3InitiateFileUpload.UploadFileMetadata> fileMetadatas = new HashMap<>();
  protected transient long currentWindowId;
  protected transient AmazonS3 s3Client;

  /**
   * Input port to receive UploadBlockMetadata
   */
  public final transient DefaultInputPort<S3BlockUpload.UploadBlockMetadata> uploadMetadataInput = new DefaultInputPort<S3BlockUpload.UploadBlockMetadata>()
  {
    @Override
    public void process(S3BlockUpload.UploadBlockMetadata tuple)
    {
      processUploadBlock(tuple);
    }
  };

  /**
   * Process to merge the uploaded block into a file.
   * @param tuple uploaded block meta data
   */
  protected void processUploadBlock(S3BlockUpload.UploadBlockMetadata tuple)
  {
    List<PartETag> listOfUploads = uploadParts.get(tuple.getKeyName());
    if (listOfUploads == null) {
      listOfUploads = new ArrayList<>();
      uploadParts.put(tuple.getKeyName(), listOfUploads);
    }
    listOfUploads.add(tuple.getPartETag());
    if (fileMetadatas.get(tuple.getKeyName()) != null) {
      emitFileMerge(tuple.getKeyName());
    }
  }

  /**
   * Input port to receive UploadFileMetadata
   */
  public final transient DefaultInputPort<S3InitiateFileUpload.UploadFileMetadata> filesMetadataInput = new DefaultInputPort<S3InitiateFileUpload.UploadFileMetadata>()
  {
    @Override
    public void process(S3InitiateFileUpload.UploadFileMetadata tuple)
    {
      processFileMetadata(tuple);
    }
  };

  /**
   * Process to merge the uploaded blocks for the given file metadata.
   * @param tuple file metadata
   */
  protected void processFileMetadata(S3InitiateFileUpload.UploadFileMetadata tuple)
  {
    String keyName = tuple.getKeyName();
    fileMetadatas.put(keyName, tuple);
    if (uploadParts.get(keyName) != null) {
      emitFileMerge(keyName);
    }
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    windowDataManager.setup(context);
    s3Client = createClient();
  }

  /**
   * Create AmazonS3 client using AWS credentials
   * @return AmazonS3
   */
  protected AmazonS3 createClient()
  {
    AmazonS3 client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretAccessKey));
    if (endPoint != null) {
      client.setEndpoint(endPoint);
    }
    return client;
  }


  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
  }

  @Override
  public void endWindow()
  {
    if (currentWindowId > windowDataManager.getLargestCompletedWindow()) {
      try {
        windowDataManager.save("UploadedFiles", currentWindowId);
      } catch (IOException e) {
        throw new RuntimeException("Unable to save recovery", e);
      }
    }
  }

  @Override
  public void teardown()
  {
    windowDataManager.teardown();
  }

  /**
   * Send the CompleteMultipartUploadRequest to S3 if all the blocks of a file are uploaded into S3.
   * @param keyName file to upload into S3
   */
  private void emitFileMerge(String keyName)
  {
    S3InitiateFileUpload.UploadFileMetadata uploadFileMetadata = fileMetadatas.get(keyName);
    List<PartETag> partETags = uploadParts.get(keyName);
    if (partETags == null || uploadFileMetadata == null ||
        uploadFileMetadata.getFileMetadata().getNumberOfBlocks() != partETags.size()) {
      return;
    }
    if (currentWindowId <= windowDataManager.getLargestCompletedWindow()) {
      uploadedFiles.add(keyName);
      return;
    }

    if (partETags.size() <= 1) {
      uploadedFiles.add(keyName);
      return;
    }

    CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName,
        keyName, uploadFileMetadata.getUploadId(), partETags);
    CompleteMultipartUploadResult result = s3Client.completeMultipartUpload(compRequest);
    if (result.getETag() != null) {
      uploadedFiles.add(keyName);
      LOG.debug("Uploaded file {} successfully", keyName);
    }
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
    if (uploadedFiles.size() > 0) {
      for (String keyName: uploadedFiles) {
        uploadParts.remove(keyName);
        fileMetadatas.remove(keyName);
      }
    }
    uploadedFiles.clear();
    try {
      windowDataManager.committed(windowId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Return the name of the bucket in which to upload the files
   * @return name of the bucket
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Sets the name of the bucket in which to upload the files.
   * @param bucketName name of the bucket
   */
  public void setBucketName(@NotNull String bucketName)
  {
    this.bucketName = Preconditions.checkNotNull(bucketName);
  }

  /**
   * Return the AWS access key
   * @return the access key
   */
  public String getAccessKey()
  {
    return accessKey;
  }

  /**
   * Sets the AWS access key
   * @param accessKey AWS access key
   */
  public void setAccessKey(@NotNull String accessKey)
  {
    this.accessKey = Preconditions.checkNotNull(accessKey);
  }

  /**
   * Returns the AWS secret access key
   * @return AWS secret access key
   */
  public String getSecretAccessKey()
  {
    return secretAccessKey;
  }

  /**
   * Sets the AWS secret access key
   * @param secretAccessKey AWS secret access key
   */
  public void setSecretAccessKey(@NotNull String secretAccessKey)
  {
    this.secretAccessKey = Preconditions.checkNotNull(secretAccessKey);
  }

  /**
   * Get the AWS S3 end point
   * @return the AWS S3 end point
   */
  public String getEndPoint()
  {
    return endPoint;
  }

  /**
   * Set the S3 end point
   * @param endPoint end point
   */
  public void setEndPoint(String endPoint)
  {
    this.endPoint = endPoint;
  }
}
