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
      return null;
    }
    return data;
  }

  @Override
  protected AbstractPartitionableKinesisInputOperator cloneOperator()
  {
    return new PartitionableKinesisSinglePortStringInputOperator();
  }


}