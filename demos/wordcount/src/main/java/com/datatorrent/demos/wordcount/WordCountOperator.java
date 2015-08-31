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
package com.datatorrent.demos.wordcount;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.util.BaseNumberKeyValueOperator;
import com.datatorrent.lib.util.BaseUniqueKeyCounter;
import com.datatorrent.lib.util.KeyValPair;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;

class UnifierHashMapSumKeys<K, V extends Number> extends BaseNumberKeyValueOperator<K,V> implements Unifier<KeyValPair<K, V>>
{
  public HashMap<K, Double> mergedTuple = new HashMap<K, Double>();
  public final transient DefaultOutputPort<KeyValPair<K, V>> mergedport = new DefaultOutputPort<KeyValPair<K, V>>();
  @Override
  public void beginWindow(long arg0)
  {
    // TODO Auto-generated method stub
  }

  public final transient DefaultInputPort<KeyValPair<K, V>> sinput = new DefaultInputPort<KeyValPair<K, V>>()
  {
	  /**
	   * Reference counts tuples
	   */
	  @Override
      public void process(KeyValPair<K,V> tuple)
		  {
			  process(tuple);
		  }
	  @Override
	  public StreamCodec<KeyValPair<K, V>> getStreamCodec()
		  {
			  return getKeyValPairStreamCodec();
		  }
  };

  @Override
  public void process(KeyValPair<K, V> e)
  {
      Double val = mergedTuple.get(e.getKey());
      if (val == null) {
        mergedTuple.put(e.getKey(), e.getValue().doubleValue());
      }
      else {
        val += e.getValue().doubleValue();
        mergedTuple.put(e.getKey(), val);
      }
  }

  /**
   * emits mergedTuple on mergedport if it is not empty
   */
  @Override
  public void endWindow()
  {
    if (!mergedTuple.isEmpty()) {
      for (Map.Entry<K, Double> e: mergedTuple.entrySet()) {
	    mergedport.emit(new KeyValPair<K, V>(e.getKey(), getValue(e.getValue())));
      }
      mergedTuple = new HashMap<K, Double>();
    }
  }

  /**
   * a no-op
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
  }

  /**
   * a noop
   */
  @Override
  public void teardown()
  {
  }
}
class UniqueCounter<K> extends BaseUniqueKeyCounter<K>
{

  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>()
  {
    /**
     * Reference counts tuples
     */
    @Override
    public void process(K tuple)
    {
      processTuple(tuple);
    }

  };

  public final transient DefaultOutputPort<KeyValPair<K, Integer>> count = new DefaultOutputPort<KeyValPair<K, Integer>>()
  {
    @Override
    public Unifier<KeyValPair<K, Integer>> getUnifier()
    {
      return new UnifierHashMapSumKeys<K, Integer>();
    }
  };

  public void beginWindow(long arg0)
  {
	super.beginWindow(arg0);
    // TODO Auto-generated method stub
  }


  /**
   * Emits one HashMap as tuple
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<K, MutableInt> e: map.entrySet()) {
      count.emit(new KeyValPair<K,Integer>(e.getKey(), e.getValue().toInteger()));
    }
    map.clear();
  }
}


@ApplicationAnnotation(name="WordCountUnifierDemo")
public class WordCountOperator implements StreamingApplication
{
   @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    WordCountInputOperator input = dag.addOperator("wordinput", new WordCountInputOperator());
    UniqueCounter<String> wordCount = dag.addOperator("count", new UniqueCounter<String>());
    dag.addStream("wordinput-count", input.outputPort, wordCount.data);
    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("count-console",wordCount.count, consoleOperator.input);
    dag.setUnifierAttribute(wordCount.count, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<UnifierHashMapSumKeys>(2));
  }
}
