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
package com.datatorrent.demos.bloomApp;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

public class BloomFilterOperator<T> extends BaseOperator
{
  protected BloomFilterOperatorObject<T> bfObj;
  private int expectedNumberOfElements;
  private float falsePositiveProbability;


  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if(this.bfObj == null) {
      bfObj = new BloomFilterOperatorObject<T>(expectedNumberOfElements, falsePositiveProbability);
    }
  }

  /**
   * Input port
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<T> data = new DefaultInputPort<T>()
  {
    /**
     * Adds the tuple into the BloomFilter
     */
    @Override
    public void process(T tuple)
    {
      processTuple(tuple);
    }
  };

  public void processTuple(T tuple)
  {
    bfObj.add(tuple);
  }

  /**
   * End window operator override.
   */
  @Override
  public void endWindow()
  {

  }

  public boolean contains(T tuple)
  {
    return bfObj.contains(tuple);

  }

  public void add(T tuple)
  {
    bfObj.add(tuple);
  }

  public void clear()
  {
    bfObj.clear();
  }
  public void setExpectedNumberOfElements(int expectedNumberOfElements)
  {
    this.expectedNumberOfElements = expectedNumberOfElements;
  }

  public void setFalsePositiveProbability(float falsePositiveProbability)
  {
    this.falsePositiveProbability = falsePositiveProbability;
  }
}
