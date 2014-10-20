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

import com.datatorrent.api.LocalMode;
import com.datatorrent.demos.bloomApp.Application;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import com.datatorrent.lib.testbench.CollectorTestSink;


/**
 *
 */
public class ApplicationTest
{
  @Test
  public void testNodeProcessing()
  {
    BloomFilterOperator<String> bfOper = new BloomFilterOperator<String>();
    Configuration conf =new Configuration(false);
    conf.addResource("dt-site-bloomApp.xml");

    bfOper.setup(null);
    bfOper.beginWindow(0);
    bfOper.data.process("Hello");
    bfOper.data.process("Operator");
    bfOper.data.process("Stream");
    bfOper.data.process("Datatorrent");
    bfOper.data.process("Operator");

    String searchString = "DataOperator";
    if(bfOper.contains(searchString))
    {
      System.out.println(searchString + " is member of BloomFilterOperator");
    } else {
      System.out.println(searchString + " is not member of BloomFilterOperator");
    }

    searchString = "Dimensions";
    if(bfOper.contains(searchString))
    {
      System.out.println(searchString + " is member of BloomFilterOperator");
    } else {
      System.out.println(searchString + " is not member of BloomFilterOperator");
    }

    searchString = "Stream";
    if(bfOper.contains(searchString))
    {
      System.out.println(searchString + " is member of BloomFilterOperator");
    } else {
      System.out.println(searchString + " is not member of BloomFilterOperator");
    }

    searchString = "data";
    if(bfOper.contains(searchString))
    {
      System.out.println(searchString + " is member of BloomFilterOperator");
    } else {
      System.out.println(searchString + " is not member of BloomFilterOperator");
    }

    bfOper.endWindow();
    bfOper.teardown();
    
  }
}
