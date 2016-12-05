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

import org.apache.apex.malhar.lib.fs.FSRecordReaderModule;

import com.datatorrent.lib.io.fs.FileSplitterInput;
import com.datatorrent.lib.io.fs.S3BlockReader;

/**
 * This module is used for reading records/tuples from S3. Records can be read
 * in parallel using multiple partitions of record reader operator. (Ordering is
 * not guaranteed when records are read in parallel)
 *
 * Input S3 directory is scanned at specified interval to poll for new data.
 *
 * The module reads data in parallel, following parameters can be configured
 * <br/>
 * 1. files: List of file(s)/directories to read. files would be in the form of
 *           SCHEME://AccessKey:SecretKey@BucketName/FileOrDirectory ,
 *           SCHEME://AccessKey:SecretKey@BucketName/FileOrDirectory , ....
 *           where SCHEME is the protocal scheme for the file system.
 *                 AccessKey is the AWS access key and SecretKey is the AWS Secret Key<br/>
 * 2. filePatternRegularExp: Files with names matching given regex will be read
 * <br/>
 * 3. scanIntervalMillis: interval between two scans to discover new files in
 * input directory<br/>
 * 4. recursive: if true, scan input directories recursively<br/>
 * 5. blockSize: block size used to read input blocks of file<br/>
 * 6. sequentialFileRead: if true, then each reader partition will read
 * different file. <br/>
 * instead of reading different offsets of the same file. <br/>
 * (File level parallelism instead of block level parallelism)<br/>
 * 7. blocksThreshold: number of blocks emitted per window<br/>
 * 8. minReaders: Minimum number of block readers for dynamic partitioning<br/>
 * 9. maxReaders: Maximum number of block readers for dynamic partitioning<br/>
 * 10. repartitionCheckInterval: Interval for re-evaluating dynamic partitioning<br/>
 * different file. <br/>
 * 11. s3EndPoint: Optional parameter used to specify S3 endpoint to use
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class S3RecordReaderModule extends FSRecordReaderModule
{
  /**
   * Endpoint for S3
   */
  private String s3EndPoint;
  /**
   * Creates an instance of FileSplitter
   *
   * @return
   */
  public FileSplitterInput createFileSplitter()
  {
    return new FileSplitterInput();
  }

  /**
   * Creates an instance of Record Reader
   *
   * @return S3RecordReader instance
   */
  @Override
  public S3RecordReader createRecordReader()
  {
    S3RecordReader s3RecordReader = new S3RecordReader();
    s3RecordReader.setBucketName(S3BlockReader.extractBucket(getFiles()));
    s3RecordReader.setAccessKey(S3BlockReader.extractAccessKey(getFiles()));
    s3RecordReader.setSecretAccessKey(S3BlockReader.extractSecretAccessKey(getFiles()));
    s3RecordReader.setEndPoint(s3EndPoint);
    s3RecordReader.setMode(this.getMode());
    s3RecordReader.setRecordLength(this.getRecordLength());
    return s3RecordReader;
  }
}
