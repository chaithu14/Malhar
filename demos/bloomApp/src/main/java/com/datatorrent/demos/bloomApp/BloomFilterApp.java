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

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.algo.bloomFilter.BloomFilterOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import org.apache.hadoop.conf.Configuration;
import com.datatorrent.lib.io.SimpleSinglePortInputOperator;
import java.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class InOperator extends SimpleSinglePortInputOperator<String> implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(InOperator.class);
  protected long averageSleep = 3000;
  protected long sleepPlusMinus = 100;
  protected String fileName = "com/datatorrent/demos/bloomApp/file.txt";

  public void setAverageSleep(long as) {
    averageSleep = as;
  }

  public void setSleepPlusMinus(long spm) {
    sleepPlusMinus = spm;
  }

  public void setFileName(String fn) {
    fileName = fn;
  }

  @Override
  public void run() {
    BufferedReader br = null;
    DataInputStream in = null;
    InputStream fstream = null;

    while (true) {
      try {
        try {
          Thread.sleep(averageSleep);
        } catch (InterruptedException ex) {
        // nothing
        }
        String line;
        fstream = this.getClass().getClassLoader().getResourceAsStream(fileName);

        in = new DataInputStream(fstream);
        br = new BufferedReader(new InputStreamReader(in));
        while ((line = br.readLine()) != null) {
          String[] words = line.trim().split("[\\p{Punct}\\s\\\"\\'“”]+");
          for (String word : words) {
            word = word.trim().toLowerCase();
            if (!word.isEmpty()) {
              //System.out.println("Sending "+word);
              outputPort.emit(word);
            }
          }
          try {
            Thread.sleep(averageSleep + (new Double(sleepPlusMinus * (Math.random() * 2 - 1))).longValue());
          } catch (InterruptedException ex) {
            // nothing
          }
        }

      } catch (IOException ex) {
        logger.debug(ex.toString());
      } finally {
        try {
          if (br != null) {
            br.close();
          }
          if (in != null) {
            in.close();
          }
          if (fstream != null) {
            fstream.close();
          }
        } catch (IOException exc) {
          // nothing
        }
      }
    }
  }
}
class BloomSearchOperator<T> extends BloomFilterOperator<T>
{

  /**
   * Input port
   */
  @InputPortFieldAnnotation(optional=true)
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    /**
     * Adds the tuple into the bloomFilter
     */

    @Override
    public void process(T tuple)
    {
      if(!contains(tuple)) {
        output1.emit(tuple);
      } else {
        output2.emit(tuple);
      }
    }
  };

  /**
   * Output port
   */
  @OutputPortFieldAnnotation(optional=true)
  public final transient DefaultOutputPort<T> output1 = new DefaultOutputPort<T>();
  @OutputPortFieldAnnotation(optional=true)
  public final transient DefaultOutputPort<T> output2 = new DefaultOutputPort<T>();
}



@ApplicationAnnotation(name="BloomFilterSearchDemo")
public class BloomFilterApp implements StreamingApplication
{
   @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    InputOperator input = dag.addOperator("wordinput", new InputOperator());
    InOperator inputSearch = dag.addOperator("find", new InOperator());
    BloomSearchOperator<String> bf = dag.addOperator("bloomFilter", new BloomSearchOperator<String>());
    dag.addStream("wordinput", input.outputPort, bf.data);
    dag.addStream("find", inputSearch.outputPort, bf.input);
    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    ConsoleOutputOperator cOperator = dag.addOperator("nconsole", new ConsoleOutputOperator());
    dag.addStream("Notfound-console",bf.output2, cOperator.input);
    dag.addStream("found-console",bf.output1, consoleOperator.input);
  }


}
