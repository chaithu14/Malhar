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

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

class BloomFilterUniqueOperator<T> extends BloomFilterOperator<T>
{
    @Override
    public void processTuple(T tuple)
    {
      if(!contains(tuple))
        unique.emit(tuple);
      add(tuple);
    }

  /**
   * Output port
   */
  @OutputPortFieldAnnotation(name = "unique")
  public final transient DefaultOutputPort<T> unique = new DefaultOutputPort<T>();

}


@ApplicationAnnotation(name="BloomFilterDemo")
public class Application implements StreamingApplication
{
   @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    InputOperator input = dag.addOperator("wordinput", new InputOperator());
    BloomFilterUniqueOperator<String> bf = dag.addOperator("bloomFilter",new BloomFilterUniqueOperator<String>());
    dag.addStream("wordinput", input.outputPort, bf.data);
    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("console",bf.unique, consoleOperator.input);

  }


}
