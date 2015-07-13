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
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.bloomFilter.BloomFilterOperator;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.bloomFilter}. <p>
 *
 */
public class BloomFilterTest
{
  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();
  @Test
  public void testGet() throws Exception
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);
    BloomFilterOperator<String> hds = new BloomFilterOperator<String>();
    hds.setExpectedNumberOfElements(100);
    hds.setFalsePositiveProbability((float) 0.01);
    hds.setup(null);

    hds.beginWindow(1);
    hds.data.process("Hello");
    hds.data.process("Operator");
    hds.data.process("Stream");
    hds.data.process("Datatorrent");
    hds.data.process("Operator");


    hds.endWindow();



    hds.teardown();

    // get fresh instance w/o cached readers
    hds = TestUtils.clone(new Kryo(), hds);
    hds.setup(null);
    hds.beginWindow(1);
    String searchString = "DataOperator";
    if(hds.contains(searchString))
    {
      System.out.println(searchString + " is member of BloomFilterOperator");
    } else {
      System.out.println(searchString + " is not member of BloomFilterOperator");
    }
    hds.endWindow();
    hds.teardown();
    //Assert.assertArrayEquals("get", data.getBytes(), val);
  }

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

