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
import com.datatorrent.api.DefaultOutputPort;

public abstract class AbstractKinesisSinglePortInputOperator<T> extends AbstractKinesisInputOperator<KinesisConsumer>
{
  /**
   * This output port emits tuples extracted from Kinesis data records.
   */
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  /**
   * Any concrete class derived from AbstractKinesisSinglePortInputOperator has to implement this method
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
    T tuple = getTuple(rc);
    if(tuple != null)
      outputPort.emit(tuple);
  }
}
