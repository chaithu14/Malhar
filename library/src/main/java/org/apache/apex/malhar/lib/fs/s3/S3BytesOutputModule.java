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

  public final transient ProxyInputPort<byte[]> input = new ProxyInputPort<byte[]>();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    S3CompactionOperator<byte[]> s3compaction =
        dag.addOperator("S3Compaction", new S3CompactionOperator());
    s3compaction.setConverter(new GenericFileOutputOperator.NoOpConverter());

    S3Reconciler s3Reconciler = dag.addOperator("S3Reconciler", new S3Reconciler());
    dag.addStream("write-to-s3", s3compaction.output, s3Reconciler.input);
  }

}
