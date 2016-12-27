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

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.esotericsoftware.kryo.NotNull;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;

@org.apache.hadoop.classification.InterfaceStability.Evolving
public class S3BytesOutputModule implements Module
{
  @NotNull
  private String accessKey;
  @NotNull
  private String secretKey;
  @NotNull
  private String bucketName;
  @NotNull
  private String directoryName;
  private Long maxLength;

  public final transient ProxyInputPort<byte[]> input = new ProxyInputPort<byte[]>();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    S3CompactionOperator<byte[]> s3compaction =
        dag.addOperator("S3Compaction", new S3CompactionOperator());
    s3compaction.setConverter(new GenericFileOutputOperator.NoOpConverter());
    if (maxLength != null) {
      s3compaction.setMaxLength(maxLength);
    }

    S3Reconciler s3Reconciler = dag.addOperator("S3Reconciler", new S3Reconciler());
    s3Reconciler.setAccessKey(accessKey);
    s3Reconciler.setSecretKey(secretKey);
    s3Reconciler.setBucketName(bucketName);
    s3Reconciler.setDirectoryName(directoryName);
    input.set(s3compaction.input);
    dag.addStream("write-to-s3", s3compaction.output, s3Reconciler.input);
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

  public Long getMaxLength()
  {
    return maxLength;
  }

  public void setMaxLength(Long maxLength)
  {
    this.maxLength = maxLength;
  }
}
