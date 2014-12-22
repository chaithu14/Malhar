package com.datatorrent.contrib.kinesis;

/**
 * Created by chaitanya on 23/12/14.
 */
import com.datatorrent.common.util.Pair;

public class KinesisStringOutputOperator extends AbstractKinesisOutputOperator<String, String>
{
  @Override
  protected byte[] getRecord(String tuple)
  {
    return tuple.getBytes();
  }
  @Override
  protected Pair<String, String> tupleToKeyValue(String tuple)
  {
    return new Pair<String, String>(String.format("partitionKey-%d", sendCount), tuple);
  }
}