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

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;


@ApplicationAnnotation(name="BloomFilterSlidingDemo")
public class BloomFilterSlidingApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    int maxValue = 30000;

    RandomEventGenerator input = dag.addOperator("rand", new RandomEventGenerator());
    input.setMinvalue(0);
    input.setMaxvalue(maxValue);

    RandomEventGenerator findGen = dag.addOperator("findVal", new RandomEventGenerator());
    findGen.setMinvalue(0);
    findGen.setMaxvalue(maxValue);
    findGen.setTuplesBlast(5);
    findGen.setTuplesBlastIntervalMillis(20);


    BloomFilterSlidingOperator<Integer> bf = dag.addOperator("bloomFilter", new BloomFilterSlidingOperator());
    dag.addStream("input", input.integer_data, bf.data);
    dag.addStream("find", findGen.integer_data, bf.find);
    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("console", bf.outputPort, consoleOperator.input);
  }


}
