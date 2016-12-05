/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.state.managed;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.utils.serde.SerializationBuffer;

import com.datatorrent.netlet.util.Slice;

public class SliceBloomFilterTest
{
  private int loop = 100000;

  @Test
  public void testBloomFilterForBytes()
  {
    final int maxSliceLength = 1000;
    Random random = new Random();
    final byte[] bytes = new byte[loop + maxSliceLength];
    random.nextBytes(bytes);

    long beginTime = System.currentTimeMillis();
    SliceBloomFilter bloomFilter = new SliceBloomFilter(100000, 0.99);
    for (int i = 0; i < loop; i++) {
      bloomFilter.put(new Slice(bytes, i, i % maxSliceLength + 1));
    }

    for (int i = 0; i < loop; i++) {
      Assert.assertTrue(bloomFilter.mightContain(new Slice(bytes, i, i % maxSliceLength + 1)));
    }
  }

  @Test
  public void testBloomFilterForInt()
  {
    testBloomFilterForInt(2);
    testBloomFilterForInt(3);
    testBloomFilterForInt(5);
    testBloomFilterForInt(7);
  }

  public void testBloomFilterForInt(int span)
  {
    double expectedFalseProbability = 0.3;
    SerializationBuffer buffer = SerializationBuffer.READ_BUFFER;

    SliceBloomFilter bloomFilter = new SliceBloomFilter(loop, expectedFalseProbability);

    for (int i = 0; i < loop; i++) {
      if (i % span == 0) {
        buffer.writeInt(i);
        bloomFilter.put(buffer.toSlice());
      }
    }
    buffer.getWindowedBlockStream().releaseAllFreeMemory();

    int falsePositive = 0;
    for (int i = 0; i < loop; i++) {
      buffer.writeInt(i);
      if (!bloomFilter.mightContain(buffer.toSlice())) {
        Assert.assertTrue(i % span != 0);
      } else {
        // BF says its present
        if (i % 2 != 0) {
          // But was not there
          falsePositive++;
        }
      }
    }
    buffer.getWindowedBlockStream().releaseAllFreeMemory();
    // Verify false positive prob
    double falsePositiveProb = falsePositive;
    falsePositiveProb /= loop;
    Assert.assertTrue(falsePositiveProb <= expectedFalseProbability);

    for (int i = 0; i < loop; i++) {
      if (i % span == 0) {
        buffer.writeInt(i);
        Assert.assertTrue(bloomFilter.mightContain(buffer.toSlice()));
      }
    }
    buffer.getWindowedBlockStream().releaseAllFreeMemory();
  }
}
