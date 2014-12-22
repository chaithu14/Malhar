package com.datatorrent.contrib.kinesis;

/**
 * Created by chaitanya on 23/12/14.
 */
import com.amazonaws.services.kinesis.model.Record;
import com.datatorrent.api.annotation.OperatorAnnotation;

import java.nio.ByteBuffer;

/**
 * Kinesis input adapter operator with a single output port, which consumes String data from the Kinesis.
 * @displayName Partitionable Kinesis Single Port String Input
 * @category Messaging
 * @tags input operator, string
 */
@OperatorAnnotation(partitionable = true)
public class PartitionableKinesisSinglePortStringInputOperator extends AbstractPartitionableKinesisSinglePortInputOperator<String>
{
  /**
   * Implement abstract method of AbstractPartitionableKinesisSinglePortInputOperator
   * Just parse the kinesis data record as a string
   */
  @Override
  public String getTuple(Record rc)
  {
    String data = "";
    try {
      ByteBuffer bb = rc.getData();
      byte[] bytes = new byte[bb.remaining() ];
      bb.get(bytes);
      data = new String(bytes);
    }
    catch (Exception ex) {
      return data;
    }
    return data;
  }

  @Override
  protected AbstractPartitionableKinesisInputOperator cloneOperator()
  {
    return new PartitionableKinesisSinglePortStringInputOperator();
  }


}