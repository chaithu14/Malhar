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

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.algo.bloomFilter.BloomFilterOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;
import java.util.BitSet;

class BloomFilterUnique<T> extends BloomFilterOperator<T> implements Serializable
{
  public long startTime;
  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    startTime = System.currentTimeMillis();
    super.setup(context);
  }

  @Override
  public void processTuple(T tuple)
  {
    long cTime = System.currentTimeMillis();
    if(cTime - startTime >= 60000) {
      throw new RuntimeException();
    }
    System.out.println("");
    if(!contains(tuple)) {
      unique.emit(tuple);
      System.out.println("Adding a new member into the BloomFilter: " + tuple);
      add(tuple);
    }
    else
      System.out.println("Already a member of BloomFilter: " + tuple);
  }

  /**
   * Output port
   */
  @OutputPortFieldAnnotation(optional=true)
  public final transient DefaultOutputPort<T> unique = new DefaultOutputPort<T>();

}

@ApplicationAnnotation(name="BloomFilterFaultDemo")
public class BloomFilterFaultTolerance implements StreamingApplication
{
   @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    InputOperator input = dag.addOperator("wordinput", new InputOperator());
    BloomFilterUnique<String> bf = dag.addOperator("bloomFilter",new BloomFilterUnique<String>());
    dag.addStream("wordinput", input.outputPort, bf.data);
    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("console",bf.unique, consoleOperator.input);

  }

}
