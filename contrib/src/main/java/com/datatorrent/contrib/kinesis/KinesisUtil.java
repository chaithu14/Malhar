/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.kinesis;

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
    assert client != null:"Illegal client";
    DescribeStreamRequest describeRequest = new DescribeStreamRequest();
    describeRequest.setStreamName(streamName);

    DescribeStreamResult describeResponse = client.describeStream(describeRequest);
    return describeResponse.getStreamDescription().getShards();
  }

  public static List<Record> getRecords(String streamName, Integer recordsLimit, Shard shId, ShardIteratorType iteratorType, String seqNo)
      throws AmazonClientException
  {
    assert client != null:"Illegal client";
    try {
      // Create the GetShardIteratorRequest instance and sets streamName, shardId and iteratorType to it
      GetShardIteratorRequest iteratorRequest = new GetShardIteratorRequest();
      iteratorRequest.setStreamName(streamName);
      iteratorRequest.setShardId(shId.getShardId());
      iteratorRequest.setShardIteratorType(iteratorType);

      // If the iteratorType is AFTER_SEQUENCE_NUMBER, set the sequence No to the iteratorRequest
      if (ShardIteratorType.AFTER_SEQUENCE_NUMBER.equals(iteratorType))
        iteratorRequest.setStartingSequenceNumber(seqNo);

      // Get the Response from the getShardIterator service method & get the shardIterator from that response
      GetShardIteratorResult iteratorResponse = client.getShardIterator(iteratorRequest);
      // getShardIterator() specifies the position in the shard
      String iterator = iteratorResponse.getShardIterator();

      // Create the GetRecordsRequest instance and set the recordsLimit and iterator
      GetRecordsRequest getRequest = new GetRecordsRequest();
      getRequest.setLimit(recordsLimit);
      getRequest.setShardIterator(iterator);

      // Get the Response from the getRecords service method and get the data records from that response.
      GetRecordsResult getResponse = client.getRecords(getRequest);
      return getResponse.getRecords();
    } catch(AmazonClientException e)
    {
      throw e;
    }
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