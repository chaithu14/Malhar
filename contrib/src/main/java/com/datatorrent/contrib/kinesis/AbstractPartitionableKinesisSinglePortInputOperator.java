package com.datatorrent.contrib.kinesis;

/**
 * Created by chaitanya on 22/12/14.
 */
import com.amazonaws.services.kinesis.model.Record;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;


@OperatorAnnotation(partitionable = true)
public abstract class AbstractPartitionableKinesisSinglePortInputOperator<T> extends AbstractPartitionableKinesisInputOperator
{
  /**
   * This output port emits tuples extracted from Kinesis.
   */
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  /**
   * Any concrete class derived from AbstractPartitionableKinesisSinglePortInputOperator has to implement this method
   * so that it knows what type of message it is going to send to Malhar.
   * It converts a ByteBuffer message into a Tuple. A Tuple can be of any type (derived from Java Object) that
   * operator user intends to.
   *
   * @param rc
   */
  public abstract T getTuple(Record rc);

  /**
   * Implement abstract method.
   */
  @Override
  public void emitTuple(Record rc)
  {
    outputPort.emit(getTuple(rc));
  }
}