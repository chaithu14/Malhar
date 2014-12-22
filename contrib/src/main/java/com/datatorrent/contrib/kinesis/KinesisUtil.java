package com.datatorrent.contrib.kinesis;

/**
 * Created by chaitanya on 23/12/14.
 */
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;

import java.util.List;

public class KinesisUtil
{

  private static transient AmazonKinesisClient client = null;

  KinesisUtil()
  {
  }

  public static void setAWSCredentials(AWSCredentialsProvider credentialsProvider) throws Exception
  {
    if(client == null) {
      try {
        if (credentialsProvider == null)
          credentialsProvider = new DefaultAWSCredentialsProviderChain();
        client = new AmazonKinesisClient(credentialsProvider);
      } catch(Exception e)
      {
        throw new AmazonClientException("Unable to load credentials");
      }
    }
  }

  public static List<Shard> getShardList(String streamName)
  {
    DescribeStreamRequest describeRequest = new DescribeStreamRequest();
    describeRequest.setStreamName(streamName);

    DescribeStreamResult describeResponse = client.describeStream(describeRequest);
    return describeResponse.getStreamDescription().getShards();
  }

  public static List<Record> getRecords(String streamName, Integer recordsLimit, Shard shId, ShardIteratorType iteratorType, String seqNo)
  {
    GetShardIteratorRequest iteratorRequest = new GetShardIteratorRequest();
    iteratorRequest.setStreamName(streamName);
    iteratorRequest.setShardId(shId.getShardId());

    iteratorRequest.setShardIteratorType(iteratorType);
    if(ShardIteratorType.AFTER_SEQUENCE_NUMBER.equals(iteratorType))
      iteratorRequest.setStartingSequenceNumber(seqNo);

    GetShardIteratorResult iteratorResponse = client.getShardIterator(iteratorRequest);
    String iterator = iteratorResponse.getShardIterator();

    GetRecordsRequest getRequest = new GetRecordsRequest();
    getRequest.setLimit(recordsLimit);
    getRequest.setShardIterator(iterator);

    GetRecordsResult getResponse = client.getRecords(getRequest);
    return getResponse.getRecords();
  }

  public static AmazonKinesisClient getClient()
  {
    return client;
  }

  public static void setClient(AmazonKinesisClient client)
  {
    KinesisUtil.client = client;
  }
}