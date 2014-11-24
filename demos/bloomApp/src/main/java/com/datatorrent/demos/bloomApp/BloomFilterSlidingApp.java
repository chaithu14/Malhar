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

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.multiwindow.AbstractSlidingWindow;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;

class SetSlidingOperator<T> extends AbstractSlidingWindow<T, HashSet<T>>
{

  @Override
  protected void processDataTuple(T tuple)
  {
    states.get(currentCursor).add(tuple);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  @Override public HashSet<T> createWindowState()
  {
    HashSet<T> obj = states.get(currentCursor);
    if(obj == null)
      return new HashSet<T>();
    else
    {
      obj.clear();
      return obj;
    }
  }

  void processTuple(T tuple)
  {
    for(int i = 0; i < getWindowSize(); i++)
    {
      HashSet<T> obj = states.get(i);
      if(obj != null && obj.contains(tuple)) {
        outputPort.emit(tuple);
        return ;
      }
    }
  }

  @OutputPortFieldAnnotation(optional=true)
  public transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  @InputPortFieldAnnotation(optional=true)
  public final transient DefaultInputPort<T> find = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processTuple(tuple);
    }
  };
}


@ApplicationAnnotation(name="BloomFilterSlidingDemo")
public class BloomFilterSlidingApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    int maxValue = 30000;

    RandomEventGenerator input = dag.addOperator("Ids", new RandomEventGenerator());
    input.setMinvalue(0);
    input.setMaxvalue(maxValue);

    RandomEventGenerator findGen = dag.addOperator("findId", new RandomEventGenerator());
    findGen.setMinvalue(0);
    findGen.setMaxvalue(maxValue);
    findGen.setTuplesBlast(5);
    findGen.setTuplesBlastIntervalMillis(20);


    BloomFilterSlidingOperator<Integer> bf = dag.addOperator("bloomFilter", new BloomFilterSlidingOperator());
    SetSlidingOperator<Integer> hashSliding = dag.addOperator("hashSet", new SetSlidingOperator<Integer>());

    dag.addStream("input", input.integer_data, bf.data);
    //dag.addStream("input2Set", input.multi_integer_data, hashSliding.data);
    dag.addStream("input2Set", bf.tuple2Insert, hashSliding.data);
    dag.addStream("find", findGen.integer_data, bf.find);
    dag.addStream("findInSet", bf.outputPort, hashSliding.find);
    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("ContainsInSet", hashSliding.outputPort, consoleOperator.input);
  }


}
