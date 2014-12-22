package com.datatorrent.contrib.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.model.Record;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;

/**
 * Created by chaitanya on 22/12/14.
 */
public abstract class AbstractKinesisInputOperator <K extends KinesisConsumer> implements InputOperator, ActivationListener<OperatorContext>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractKinesisInputOperator.class);

  private int tuplesBlast = 1024;
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
    int bufferLength = consumer.messageSize();
    for (int i = tuplesBlast < bufferLength ? tuplesBlast : bufferLength; i-- > 0; ) {
      emitTuple(consumer.pollRecord());
    }
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

  public Integer getRecordsLimit()
  {
    return this.consumer.getRecordsLimit();
  }

  public void setRecordsLimit(Integer recordsLimit)
  {
    this.consumer.setRecordsLimit(recordsLimit);
  }

  public int getTuplesBlast()
  {
    return tuplesBlast;
  }

  public void setTuplesBlast(int tuplesBlast)
  {
    this.tuplesBlast = tuplesBlast;
  }
}
