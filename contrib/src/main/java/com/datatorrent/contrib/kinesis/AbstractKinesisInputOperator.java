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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.model.Record;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.validation.constraints.Min;

public abstract class AbstractKinesisInputOperator <K extends KinesisConsumer> implements InputOperator, ActivationListener<OperatorContext>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractKinesisInputOperator.class);

  @Min(1)
  private int maxTuplesPerWindow = Integer.MAX_VALUE;
  private int emitCount = 0;
  protected AWSCredentialsProvider credentialsProvider = null;

  @Valid
  protected KinesisConsumer consumer = new KinesisConsumer();
  /**
   * Any concrete class derived from KinesisInputOperator has to implement this method
   * so that it knows what type of record it is going to send to Malhar.
   *
   * @param record
   */
  protected abstract void emitTuple(Record record);

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
      throw new RuntimeException(e.getMessage());
    }
    consumer.create();
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
    consumer.teardown();
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    emitCount = 0;
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
  }

  /**
   * Implement ActivationListener Interface.
   */
  @Override
  public void activate(OperatorContext ctx)
  {
    consumer.start();
  }

  /**
   * Implement ActivationListener Interface.
   */
  @Override
  public void deactivate()
  {
    consumer.stop();
  }

  /**
   * Implement InputOperator Interface.
   */
  @Override
  public void emitTuples()
  {
    int count = consumer.messageSize();
    if(maxTuplesPerWindow > 0)
      count = Math.min(count, maxTuplesPerWindow - emitCount);
    for (int i = 0; i < count; i++) {
      emitTuple(consumer.pollRecord());
    }
    emitCount += count;
  }

  public void setConsumer(K consumer)
  {
    this.consumer = consumer;
  }

  public KinesisConsumer getConsumer()
  {
    return consumer;
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
    return this.consumer.getStreamName();
  }

  public void setStreamName(String streamName)
  {
    this.consumer.setStreamName(streamName);
  }

  public int getMaxTuplesPerWindow()
  {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(int maxTuplesPerWindow)
  {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }
}
