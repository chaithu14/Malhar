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

package org.apache.apex.malhar.lib.fs;

import java.io.IOException;
import java.util.Arrays;

import javax.validation.constraints.Pattern;

import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.fs.FSDataInputStream;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.esotericsoftware.kryo.NotNull;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.FSSliceReader;
import com.datatorrent.lib.io.block.ReaderContext;

/**
 * This operator can be used for reading records/tuples from S3 in parallel
 * (without ordering guarantees between tuples). Records can be delimited (e.g.
 * newline) or fixed width records. Output tuples are byte[].
 *
 * Typically, this operator will be connected to output of FileSplitterInput to
 * read records in parallel.
 */
@Evolving
public class S3RecordReader extends FSSliceReader
{
  private transient AmazonS3 s3Client;
  private String endPoint;
  @NotNull
  private String bucketName;
  @NotNull
  private String accessKey;
  @NotNull
  private String secretAccessKey;
  private int overflowBufferSize;

  /**
   * Record reader mode decides how to split the records.
   */
  public static enum RECORD_READER_MODE
  {
    DELIMITED_RECORD, FIXED_WIDTH_RECORD;
  }

  /**
   * Criteria for record split
   */
  private RECORD_READER_MODE mode = RECORD_READER_MODE.DELIMITED_RECORD;

  /**
   * Length for fixed width record
   */
  private int recordLength;

  /**
   * Port to emit individual records/tuples as byte[]
   */
  public final transient DefaultOutputPort<byte[]> records = new DefaultOutputPort<byte[]>();

  public S3RecordReader()
  {
    /*
     * Set default overflowBufferSize to 1MB
     */
    overflowBufferSize = 1024 * 1024;
  }

  /**
   * Extract the file path from given block and set it to the readerContext
   *
   * @param block
   *          block metadata
   * @return stream
   * @throws IOException
   */
  @Override
  protected FSDataInputStream setupStream(BlockMetadata.FileBlockMetadata block) throws IOException
  {
    String filePath = block.getFilePath();
    // File path would be the path after bucket name.
    // Check if the file path starts with "/"
    if (filePath.startsWith("/")) {
      filePath = filePath.substring(1);
    }

    if (mode == RECORD_READER_MODE.FIXED_WIDTH_RECORD) {
      ((S3FixedWidthRecordReaderContext)readerContext).setFilePath(filePath);
      ((S3FixedWidthRecordReaderContext)readerContext).setFileLength(block.getFileLength());
    } else {
      ((S3DelimitedRecordReaderContext)readerContext).setFilePath(filePath);
      ((S3DelimitedRecordReaderContext)readerContext).setFileLength(block.getFileLength());
    }
    return null;
  }

  /**
   * Initialize the reader context
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    s3Client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretAccessKey));
    if (endPoint != null) {
      s3Client.setEndpoint(endPoint);
    }
    if (mode == RECORD_READER_MODE.FIXED_WIDTH_RECORD) {
      S3FixedWidthRecordReaderContext fixedBytesReaderContext = new S3FixedWidthRecordReaderContext();
      fixedBytesReaderContext.setLength(recordLength);
      fixedBytesReaderContext.setBucketName(bucketName);
      fixedBytesReaderContext.setS3Client(s3Client);
      readerContext = fixedBytesReaderContext;
    } else {
      S3DelimitedRecordReaderContext delimitedRecordReaderContext = new S3DelimitedRecordReaderContext();
      delimitedRecordReaderContext.setBucketName(bucketName);
      delimitedRecordReaderContext.setS3Client(s3Client);
      readerContext = delimitedRecordReaderContext;
    }
  }

  /**
   * Read the block data and emit records based on reader context
   *
   * @param blockMetadata
   *          contains the metadata information of the block of the file
   * @throws IOException
   */
  protected void readBlock(BlockMetadata blockMetadata) throws IOException
  {
    readerContext.initialize(stream, blockMetadata, consecutiveBlock);
    if (mode == RECORD_READER_MODE.FIXED_WIDTH_RECORD) {
      int bytesRead = ((S3FixedWidthRecordReaderContext)readerContext).getBlockFromS3();
      if (bytesRead == -1) {
        return;
      }
      ((S3FixedWidthRecordReaderContext)readerContext).setBufferOffset(0);
    } else {
      /*
       * Initialize the bufferSize and overflowBufferSize
       */
      int bufferSize = Long.valueOf(blockMetadata.getLength() - blockMetadata.getOffset()).intValue();
      ((S3DelimitedRecordReaderContext)readerContext).setBufferSize(bufferSize);
      if (overflowBufferSize > bufferSize) {
        ((S3DelimitedRecordReaderContext)readerContext).setOverflowBufferSize(bufferSize);
      } else {
        ((S3DelimitedRecordReaderContext)readerContext).setOverflowBufferSize(overflowBufferSize);
      }
    }

    ReaderContext.Entity entity;
    while ((entity = readerContext.next()) != null) {
      counters.getCounter(ReaderCounterKeys.BYTES).add(entity.getUsedBytes());
      byte[] record = entity.getRecord();

      if (record != null) {
        counters.getCounter(ReaderCounterKeys.RECORDS).increment();
        records.emit(record);
      }
    }
  }

  /**
   * RecordReaderContext for reading delimited S3 Records.
   */
  private static class S3DelimitedRecordReaderContext
      extends ReaderContext.ReadAheadLineReaderContext<FSDataInputStream>
  {
    /**
     * Amazon client used to read bytes from S3
     */
    private transient AmazonS3 s3Client;
    /**
     * S3 bucket name
     */
    private transient String bucketName;
    /**
     * path of file being processed in bucket
     */
    private transient String filePath;
    /**
     * length of the file being processed
     */
    private transient long fileLength;

    /**
     * S3 block read would be achieved through the AmazonS3 client. Following
     * are the steps to achieve: (1) Create the objectRequest from bucketName
     * and filePath. (2) Set the range to the above created objectRequest. (3)
     * Get the object portion through AmazonS3 client API. (4) Get the object
     * content from the above object portion.
     *
     * @param bytesFromCurrentOffset
     *          bytes read till now from current offset
     * @param bytesToFetch
     *          the number of bytes to be fetched
     * @return the number of bytes read, -1 if 0 bytes read
     * @throws IOException
     */

    @Override
    protected int readData(final long bytesFromCurrentOffset, final int bytesToFetch) throws IOException
    {
      GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, filePath);
      rangeObjectRequest.setRange(offset + bytesFromCurrentOffset, offset + bytesFromCurrentOffset + bytesToFetch - 1);
      S3Object objectPortion = s3Client.getObject(rangeObjectRequest);
      S3ObjectInputStream wrappedStream = objectPortion.getObjectContent();
      buffer = ByteStreams.toByteArray(wrappedStream);
      wrappedStream.close();
      int bufferLength = buffer.length;
      if (bufferLength <= 0) {
        return -1;
      }
      return bufferLength;
    }

    @Override
    protected boolean checkEndOfStream(final long usedBytesFromOffset)
    {
      if ((offset + usedBytesFromOffset) >= fileLength) {
        return true;
      }
      return false;
    }

    /**
     * Set the AmazonS3 service
     *
     * @param s3Client
     *          given s3Client
     */
    public void setS3Client(@javax.validation.constraints.NotNull AmazonS3 s3Client)
    {
      Preconditions.checkNotNull(s3Client);
      this.s3Client = s3Client;
    }

    /**
     * Set the bucket name
     *
     * @param bucketName
     *          given bucketName
     */
    public void setBucketName(@javax.validation.constraints.NotNull String bucketName)
    {
      Preconditions.checkNotNull(bucketName);
      this.bucketName = bucketName;
    }

    /**
     * Sets the file path
     *
     * @param filePath
     *          given filePath
     */
    public void setFilePath(String filePath)
    {
      this.filePath = filePath;
    }

    /**
     * @param fileLength
     *          length of the file to which the block belongs
     */
    public void setFileLength(long fileLength)
    {
      this.fileLength = fileLength;
    }

  }

  /**
   * RecordReaderContext for reading fixed width S3 Records.
   */
  private static class S3FixedWidthRecordReaderContext extends ReaderContext.FixedBytesReaderContext<FSDataInputStream>
  {
    /**
     * Amazon client used to read bytes from S3
     */
    private transient AmazonS3 s3Client;
    /**
     * S3 bucket name
     */
    private transient String bucketName;
    /**
     * path of file being processed in bucket
     */
    private transient String filePath;
    /**
     * length of the file being processed
     */
    private transient long fileLength;

    /**
     * used to hold data retrieved from S3
     */
    private transient byte[] buffer;

    /**
     * current offset within the byte[] buffer
     */
    private transient int bufferOffset;

    /**
     * S3 block read would be achieved through the AmazonS3 client. Following
     * are the steps to achieve: (1) Create the objectRequest from bucketName
     * and filePath. (2) Set the range to the above created objectRequest. Set
     * the range so that it gets aligned with the fixed width records. (3) Get
     * the object portion through AmazonS3 client API. (4) Get the object
     * content from the above object portion.
     */
    protected int getBlockFromS3() throws IOException
    {
      long startOffset = blockMetadata.getOffset()
          + (this.length - (blockMetadata.getOffset() % this.length)) % this.length;
      long endOffset = blockMetadata.getLength()
          + ((this.length - (blockMetadata.getLength() % this.length)) % this.length) - 1;
      if (endOffset == (startOffset - 1) || startOffset > this.fileLength) {
        /*
         * If start and end offset is same, it means no record starts in this block
         */
        return -1;
      }
      if (endOffset >= this.fileLength) {
        endOffset = this.fileLength - 1;
      }
      offset = startOffset;
      GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, filePath);
      rangeObjectRequest.setRange(startOffset, endOffset);
      S3Object objectPortion = s3Client.getObject(rangeObjectRequest);
      S3ObjectInputStream wrappedStream = objectPortion.getObjectContent();
      buffer = ByteStreams.toByteArray(wrappedStream);
      wrappedStream.close();
      return buffer.length;
    }

    @Override
    protected ReaderContext.Entity readEntity() throws IOException
    {
      entity.clear();
      /*
       * In case file length is not a multiple of record length, the last record may not have length = recordLength.
       * The data to be read from buffer array should be less in this case.
       */
      long bufferLength = length;
      if (offset + length > fileLength) {
        bufferLength = fileLength - offset;
      }
      byte[] record = Arrays.copyOfRange(buffer, Long.valueOf(bufferOffset).intValue(),
          Long.valueOf(bufferOffset + bufferLength).intValue());
      bufferOffset += record.length;
      entity.setRecord(record);
      entity.setUsedBytes(record.length);
      return entity;
    }

    /**
     * Set the AmazonS3 service
     *
     * @param s3Client
     *          given s3Client
     */
    public void setS3Client(AmazonS3 s3Client)
    {
      this.s3Client = s3Client;
    }

    /**
     * Set the bucket name
     *
     * @param bucketName
     *          given bucketName
     */
    public void setBucketName(String bucketName)
    {
      this.bucketName = bucketName;
    }

    /**
     * Sets the file path
     *
     * @param filePath
     *          given filePath
     */
    public void setFilePath(String filePath)
    {
      this.filePath = filePath;
    }

    /**
     * @param fileLength
     *          length of the file to which the block belongs
     */
    public void setFileLength(long fileLength)
    {
      this.fileLength = fileLength;
    }

    /**
     * Sets the offset within the current buffer
     *
     * @param bufferOffset
     *          offset within the current buffer
     */
    protected void setBufferOffset(int bufferOffset)
    {
      this.bufferOffset = bufferOffset;
    }
  }

  /**
   * Size of bytes to be retrieved when a record overflows
   *
   * return overflowBufferSize
   */
  public int getOverflowBufferSize()
  {
    return overflowBufferSize;
  }

  /**
   * Size of bytes to be retrieved when a record overflows
   *
   * @param overflowBufferSize
   */
  public void setOverflowBufferSize(int overflowBufferSize)
  {
    this.overflowBufferSize = overflowBufferSize;
  }

  /**
   * Get the S3 bucket name
   *
   * @return bucket
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Set the bucket name where the file resides
   *
   * @param bucketName
   *          bucket name
   */
  public void setBucketName(String bucketName)
  {
    this.bucketName = bucketName;
  }

  /**
   * Return the access key
   *
   * @return the accessKey
   */
  public String getAccessKey()
  {
    return accessKey;
  }

  /**
   * Set the access key
   *
   * @param accessKey
   *          given accessKey
   */
  public void setAccessKey(String accessKey)
  {
    this.accessKey = accessKey;
  }

  /**
   * Return the secretAccessKey
   *
   * @return the secretAccessKey
   */
  public String getSecretAccessKey()
  {
    return secretAccessKey;
  }

  /**
   * Set the secretAccessKey
   *
   * @param secretAccessKey
   *          secretAccessKey
   */
  public void setSecretAccessKey(String secretAccessKey)
  {
    this.secretAccessKey = secretAccessKey;
  }

  /**
   * Criteria for record split : FIXED_WIDTH_RECORD or DELIMITED_RECORD
   *
   * @param mode
   *          Mode
   */
  public void setMode(
      @Pattern(regexp = "FIXED_WIDTH_RECORD|DELIMITED_RECORD", flags = Pattern.Flag.CASE_INSENSITIVE) String mode)
  {
    this.mode = RECORD_READER_MODE.valueOf(mode.toUpperCase());
  }

  /**
   * Criteria for record split
   *
   * @return mode
   */
  public String getMode()
  {
    return mode.toString();
  }

  /**
   * Length for fixed width record
   *
   * @param recordLength
   */
  public void setRecordLength(int recordLength)
  {
    if (mode == RECORD_READER_MODE.FIXED_WIDTH_RECORD && recordLength <= 0) {
      throw new IllegalArgumentException("recordLength should be greater than 0.");
    }
    this.recordLength = recordLength;
  }

  /**
   * Length for fixed width record
   *
   * @return record length
   */
  public int getRecordLength()
  {
    return recordLength;
  }

  /**
   * S3 endpoint
   *
   * @param s3
   *          endpoint
   */
  public void setEndPoint(String endPoint)
  {
    this.endPoint = endPoint;
  }

  /**
   * S3 endpoint
   *
   * @return s3 endpoint
   */
  public String getEndPoint()
  {
    return endPoint;
  }
}
