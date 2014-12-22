package com.datatorrent.contrib.kinesis;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by chaitanya on 22/12/14.
 */
public abstract class AbstractKinesisOutputOperator<V, T> implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractKinesisOutputOperator.class);

  protected String streamName;
  protected AWSCredentialsProvider credentialsProvider = null;
  protected static transient AmazonKinesisClient client = null;
  protected int sendCount;
  protected boolean isBatchProcessing = true;

  protected abstract byte[] getRecord(V tuple);
  protected abstract Pair<String, V> tupleToKeyValue(T tuple);
  protected int tupleIdx = 0;
  List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<PutRecordsRequestEntry>();
  // Max size of each record: 50KB, Max size of putRecords: 4.5MB
  // So, default capacity would be 4.5MB/50KB = 92
  @Min(2)
  @Max(500)
  protected int capacity = 92;
  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
    if(isBatchProcessing && tupleIdx != 0)
    {
      PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
      putRecordsRequest.setStreamName(streamName);
      putRecordsRequest.setRecords(putRecordsRequestEntryList);
      PutRecordsResult putRecordsResult = client.putRecords(putRecordsRequest);
      putRecordsRequestEntryList.clear();
      tupleIdx = 0;
    }
  }

  /**
   * Implement Component Interface.
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    try {
      KinesisUtil.setAWSCredentials(credentialsProvider);
    } catch(Exception e)
    {
      logger.error(e.getMessage());
    }
    this.setClient(KinesisUtil.getClient());
    if(isBatchProcessing)
    {
      putRecordsRequestEntryList.clear();
      tupleIdx = 0;
    }
  }

  /**
   * This input port receives tuples that will be written out to Kinesis.
   */
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      // Send out single data
      try {
        if(isBatchProcessing)
        {
          if(tupleIdx == capacity)
          {
            PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
            putRecordsRequest.setStreamName(streamName);
            putRecordsRequest.setRecords(putRecordsRequestEntryList);
            PutRecordsResult putRecordsResult = client.putRecords(putRecordsRequest);
            putRecordsRequestEntryList.clear();
            tupleIdx = 0;
          }
          Pair<String, V> keyValue = tupleToKeyValue(tuple);
          PutRecordsRequestEntry putRecordsEntry = new PutRecordsRequestEntry();
          putRecordsEntry.setData(ByteBuffer.wrap(getRecord(keyValue.second)));
          putRecordsEntry.setPartitionKey(keyValue.first);
          putRecordsRequestEntryList.add(putRecordsEntry);
          tupleIdx++;

        } else {
          Pair<String, V> keyValue = tupleToKeyValue(tuple);
          PutRecordRequest requestRecord = new PutRecordRequest();
          requestRecord.setStreamName(streamName);
          requestRecord.setPartitionKey(keyValue.first);
          requestRecord.setData(ByteBuffer.wrap(getRecord(keyValue.second)));

          client.putRecord(requestRecord);
        }
        sendCount++;
      } catch (AmazonClientException e) {
        logger.debug("Exception Caught");
        throw e;
        //e.printStackTrace();
      }
    }
  };

  public void setClient(AmazonKinesisClient _client)
  {
    client = _client;
  }

  public AWSCredentialsProvider getCredentialsProvider()
  {
    return credentialsProvider;
  }

  public void setCredentialsProvider(AWSCredentialsProvider credentialsProvider)
  {
    this.credentialsProvider = credentialsProvider;
  }

  public String getStreamName()
  {
    return streamName;
  }

  public void setStreamName(String streamName)
  {
    this.streamName = streamName;
  }
}
