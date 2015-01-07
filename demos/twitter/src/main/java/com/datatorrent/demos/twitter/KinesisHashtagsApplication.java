/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.twitter;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kinesis.AbstractPartitionableKinesisInputOperator;
import com.datatorrent.contrib.kinesis.KinesisStringOutputOperator;
import com.datatorrent.contrib.kinesis.PartitionableKinesisSinglePortStringInputOperator;
import com.datatorrent.contrib.kinesis.ShardManager;
import com.datatorrent.contrib.twitter.TwitterSampleInput;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

@ApplicationAnnotation(name="KinesisDemo")
public class KinesisHashtagsApplication implements StreamingApplication
{
  private final Locality locality = null;

  private InputPort<Object> consoleOutput(DAG dag, String operatorName)
  {
    String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    if (!StringUtils.isEmpty(gatewayAddress)) {
      URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
      String topic = "demos.twitter." + operatorName;
      //LOG.info("WebSocket with gateway at: {}", gatewayAddress);
      PubSubWebSocketOutputOperator<Object> wsOut = dag.addOperator(operatorName, new PubSubWebSocketOutputOperator<Object>());
      wsOut.setUri(uri);
      wsOut.setTopic(topic);
      return wsOut.input;
    }
    ConsoleOutputOperator operator = dag.addOperator(operatorName, new ConsoleOutputOperator());
    operator.setStringFormat(operatorName + ": %s");
    return operator.input;
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //dag.setAttribute(DAG.APPLICATION_NAME, "TwitterTrendingHashtagsApplication");

    // Setup the operator to get the data from twitter sample stream injected into the system.
    TwitterSampleInput twitterFeed = new TwitterSampleInput();
    twitterFeed = dag.addOperator("TweetSampler", twitterFeed);

    //  Setup the operator to get the Hashtags extracted from the twitter statuses
    TwitterStatusHashtagExtractor HashtagExtractor = dag.addOperator("HashtagExtractor", TwitterStatusHashtagExtractor.class);

    //Setup the operator send the twitter statuses to kinesis
    KinesisStringOutputOperator outputOp = dag.addOperator("ToKinesis", new KinesisStringOutputOperator());
    outputOp.setBatchSize(500);

    // Feed the statuses from feed into the input of the Hashtag extractor.
    dag.addStream("TweetStream", twitterFeed.status, HashtagExtractor.input).setLocality(Locality.CONTAINER_LOCAL);
    //  Start counting the Hashtags coming out of Hashtag extractor
    dag.addStream("SendToKinesis", HashtagExtractor.hashtags, outputOp.inputPort).setLocality(locality);

    //------------------------------------------------------------------------------------------

    PartitionableKinesisSinglePortStringInputOperator inputOp = dag.addOperator("FromKinesis", new PartitionableKinesisSinglePortStringInputOperator());
    ShardManager shardStats = new ShardManager();
    inputOp.setShardManager(shardStats);
    inputOp.getConsumer().setRecordsLimit(600);
    inputOp.setStrategy(AbstractPartitionableKinesisInputOperator.PartitionStrategy.MANY_TO_ONE.toString());

    // Setup a node to count the unique Hashtags within a window.
    UniqueCounter<String> uniqueCounter = dag.addOperator("UniqueHashtagCounter", new UniqueCounter<String>());

    // Get the aggregated Hashtag counts and count them over last 5 mins.
    WindowedTopCounter<String> topCounts = dag.addOperator("TopCounter", new WindowedTopCounter<String>());
    topCounts.setTopCount(10);
    topCounts.setSlidingWindowWidth(600, 1);

    dag.addStream("TwittedHashtags", inputOp.outputPort, uniqueCounter.data).setLocality(locality);

    // Count unique Hashtags
    dag.addStream("UniqueHashtagCounts", uniqueCounter.count, topCounts.input).setLocality(locality);
    // Count top 10
    dag.addStream("TopHashtags", topCounts.output, consoleOutput(dag, "topHashtags")).setLocality(locality);
  }
}

