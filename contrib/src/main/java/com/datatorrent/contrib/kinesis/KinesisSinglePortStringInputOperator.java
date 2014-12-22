package com.datatorrent.contrib.kinesis;

/**
 * Created by chaitanya on 23/12/14.
 */
import com.amazonaws.services.kinesis.model.Record;

import java.nio.ByteBuffer;

public class KinesisSinglePortStringInputOperator extends AbstractKinesisSinglePortInputOperator<String>
{

  /**
   * Implement abstract method of AbstractKinesisSinglePortInputOperator
   */
  @Override
  public String getTuple(Record record)
  {
    String data = "";
    try {
      ByteBuffer bb = record.getData();
      byte[] bytes = new byte[bb.remaining() ];
      bb.get(bytes);
      data = new String(bytes);
    }
    catch (Exception ex) {
      return data;
    }
    return data;
  }

}