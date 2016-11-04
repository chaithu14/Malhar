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

import javax.validation.constraints.NotNull;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.lib.io.block.AbstractBlockReader;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.fs.AbstractFileSplitter;
import com.datatorrent.netlet.util.Slice;

/**
 * S3OutputModule can be used to upload the files/directory into S3. This module supports
 * parallel uploads of multiple blocks of the same file and merge those blocks in sequence.
 *
 * Below operators are wrapped into single component using Module API
 *  - S3InitiateFileUpload
 *  - S3BlockUpload
 *  - S3FileMerger
 */
@InterfaceStability.Evolving
public class S3OutputModule implements Module
{
  /**
   * AWS access key
   */
  @NotNull
  private String accessKey;
  /**
   * AWS secret access key
   */
  @NotNull
  private String secretAccessKey;

  /**
   * S3 End point
   */
  private String endPoint;
  /**
   * Name of the bucket in which to upload the files
   */
  @NotNull
  private String bucketName;

  /**
   * Path of the output directory. Relative path of the files copied will be
   * maintained w.r.t. source directory and output directory
   */
  @NotNull
  private String outputDirectoryPath;

  /**
   * Input port for files metadata.
   */
  public final transient ProxyInputPort<AbstractFileSplitter.FileMetadata> filesMetadataInput = new ProxyInputPort<AbstractFileSplitter.FileMetadata>();

  /**
   * Input port for blocks metadata
   */
  public final transient ProxyInputPort<BlockMetadata.FileBlockMetadata> blocksMetadataInput = new ProxyInputPort<BlockMetadata.FileBlockMetadata>();

  /**
   * Input port for blocks data
   */
  public final transient ProxyInputPort<AbstractBlockReader.ReaderRecord<Slice>> blockData = new ProxyInputPort<AbstractBlockReader.ReaderRecord<Slice>>();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // DAG for S3 Output Module as follows:
    //   ---- S3InitiateFileUpload -----|
    //             |                    S3FileMerger
    //   ----  S3BlockUpload ------------|

    S3InitiateFileUpload initiateUpload = dag.addOperator("InitiateUpload", new S3InitiateFileUpload());
    initiateUpload.setAccessKey(accessKey);
    initiateUpload.setSecretAccessKey(secretAccessKey);
    initiateUpload.setBucketName(bucketName);
    initiateUpload.setOutputDirectoryPath(outputDirectoryPath);

    S3BlockUpload blockUploader = dag.addOperator("BlockUpload", new S3BlockUpload());
    blockUploader.setAccessKey(accessKey);
    blockUploader.setSecretAccessKey(secretAccessKey);
    blockUploader.setBucketName(bucketName);

    S3FileMerger fileMerger = dag.addOperator("FileMerger", new S3FileMerger());
    fileMerger.setAccessKey(accessKey);
    fileMerger.setSecretAccessKey(secretAccessKey);
    fileMerger.setBucketName(bucketName);

    if (endPoint != null) {
      initiateUpload.setEndPoint(endPoint);
      blockUploader.setEndPoint(endPoint);
      fileMerger.setEndPoint(endPoint);
    }

    dag.setInputPortAttribute(blockUploader.blockInput, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(blockUploader.blockMetadataInput, Context.PortContext.PARTITION_PARALLEL, true);
    // Add Streams
    dag.addStream("InitiateUploadIDToMerger", initiateUpload.fileMetadataOutput, fileMerger.filesMetadataInput);
    dag.addStream("InitiateUploadIDToWriter", initiateUpload.uploadMetadataOutput, blockUploader.uploadMetadataInput);
    dag.addStream("WriterToMerger", blockUploader.output, fileMerger.uploadMetadataInput);

    // Set the proxy ports
    filesMetadataInput.set(initiateUpload.filesMetadataInput);
    blocksMetadataInput.set(blockUploader.blockMetadataInput);
    blockData.set(blockUploader.blockInput);
  }

  /**
   * Get the AWS access key
   * @return AWS access key
   */
  public String getAccessKey()
  {
    return accessKey;
  }

  /**
   * Set the AWS access key
   * @param accessKey access key
   */
  public void setAccessKey(String accessKey)
  {
    this.accessKey = accessKey;
  }

  /**
   * Return the AWS secret access key
   * @return AWS secret access key
   */
  public String getSecretAccessKey()
  {
    return secretAccessKey;
  }

  /**
   * Set the AWS secret access key
   * @param secretAccessKey AWS secret access key
   */
  public void setSecretAccessKey(String secretAccessKey)
  {
    this.secretAccessKey = secretAccessKey;
  }


  /**
   * Get the name of the bucket in which to upload the files
   * @return bucket name
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Set the name of the bucket in which to upload the files
   * @param bucketName name of the bucket
   */
  public void setBucketName(String bucketName)
  {
    this.bucketName = bucketName;
  }

  /**
   * Return the S3 End point
   * @return S3 End point
   */
  public String getEndPoint()
  {
    return endPoint;
  }

  /**
   * Set the S3 End point
   * @param endPoint S3 end point
   */
  public void setEndPoint(String endPoint)
  {
    this.endPoint = endPoint;
  }

  /**
   * Get the path of the output directory.
   * @return path of output directory
   */
  public String getOutputDirectoryPath()
  {
    return outputDirectoryPath;
  }

  /**
   * Set the path of the output directory.
   * @param outputDirectoryPath path of output directory
   */
  public void setOutputDirectoryPath(String outputDirectoryPath)
  {
    this.outputDirectoryPath = outputDirectoryPath;
  }
}
