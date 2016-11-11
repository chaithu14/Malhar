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
package org.apache.apex.malhar.lib.state.spillable;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.apex.malhar.lib.state.managed.KeyBucketExtractor;
import org.apache.apex.malhar.lib.state.managed.ManagedStateTestUtils;
import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedTimeStateSpillableStore;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SerializationBuffer;
import org.apache.apex.malhar.lib.utils.serde.StringSerde;
import org.apache.apex.malhar.lib.utils.serde.WindowedBlockStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.esotericsoftware.kryo.io.Input;

import com.datatorrent.api.Context;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.netlet.util.Slice;

import static org.apache.apex.malhar.lib.state.spillable.SpillableTestUtils.STRING_SERDE;

public class SpillableTimeSlicedMapImplTest
{
  private static final String APPLICATION_PATH_PREFIX = "target/SpillableTimeSlicedMapImplTest/";
  public static final byte[] ID1 = new byte[]{(byte)0};
  public static final byte[] ID2 = new byte[]{(byte)1};

  public static final TestStringTimeExtractor TE = new TestStringTimeExtractor();

  private SpillableTimeStateStore store;
  public Context.OperatorContext operatorContext;
  public String applicationPath;

  private TimeExtractor<String> te = null;
  private KeyBucketExtractor<String> ke = null;


  @Before
  public void beforeTest()
  {
    store = new ManagedTimeStateSpillableStore();
    applicationPath =  OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    ((FileAccessFSImpl)((ManagedTimeStateSpillableStore)store).getFileAccess()).setBasePath(applicationPath + "/" + "bucket_data");
    operatorContext = ManagedStateTestUtils.getOperatorContext(1, applicationPath);
    te = TE;
    ke = new StringKeyBucketExtractor();
  }

  public class StringKeyBucketExtractor implements KeyBucketExtractor<String>
  {
    @Override
    public long getBucket(String s)
    {
      return 0;
    }
  }

  @After
  public void afterTest()
  {
    Path root = new Path(applicationPath);
    try {
      FileSystem fs = FileSystem.newInstance(root.toUri(), new Configuration());
      fs.delete(root, true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void simpleGetAndPutTest()
  {
    SpillableTimeSlicedMapImpl<String, String> map = new SpillableTimeSlicedMapImpl<>(store,ID1,new StringSerde(), new StringSerde(), te, ke);

    store.setup(operatorContext);
    map.setup(operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    Assert.assertEquals(0, map.size());

    map.put("a", "1");
    map.put("b", "2");
    map.put("c", "3");

    Assert.assertEquals(3, map.size());

    assertMultiEqualsFromMap(map, new String[]{"1", "2", "3", null}, new String[]{"a", "b", "c", "d"});

    multiValueCheck(new String[]{"a", "b", "c", "d"}, ID1, new String[]{null, null, null, null});

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map.beginWindow(windowId);

    multiValueCheck(new String[]{"a", "b", "c", "d"}, ID1, new String[]{"1", "2", "3", null});

    Assert.assertEquals(3, map.size());

    assertMultiEqualsFromMap(map, new String[]{"1", "2", "3", null}, new String[]{"a", "b", "c", "d"});

    map.put("d", "4");
    map.put("e", "5");
    map.put("f", "6");

    Assert.assertEquals(6, map.size());

    assertMultiEqualsFromMap(map, new String[]{"4", "5", "6"}, new String[]{"d", "e", "f"});

    multiValueCheck(new String[]{"a", "b", "c", "d", "e", "f"}, ID1, new String[]{"1", "2", "3", null, null, null});

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);
    map.beginWindow(windowId);


    multiValueCheck(new String[]{"a", "b", "c", "d", "e", "f", "g"}, ID1, new String[]{"1", "2", "3", "4", "5", "6", null});

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    map.teardown();
    store.teardown();
  }

  private void multiValueCheck(String[] keys, byte[] samePrefix, String[] expectedVal)
  {
    for (int i = 0; i < keys.length; i++) {
      checkValue(store, _bid(keys[i], ke), keys[i], samePrefix, expectedVal[i]);
    }
  }

  private void assertMultiEqualsFromMap(SpillableTimeSlicedMapImpl<String, String> map, String[] expectedV, String[] keys)
  {
    for (int i = 0; i < expectedV.length; i++) {
      Assert.assertEquals(expectedV[i], map.get(keys[i]));
    }
  }

  private long _bid(String key, KeyBucketExtractor te)
  {
    if (te != null) {
      return te.getBucket(key);
    } else {
      return 0L;
    }
  }

  protected static SerializationBuffer buffer = new SerializationBuffer(new WindowedBlockStream());

  public static void checkValue(SpillableTimeStateStore store, long bucketId, String key,
      byte[] prefix, String expectedValue)
  {
    buffer.writeBytes(prefix);
    STRING_SERDE.serialize(key, buffer);
    checkValue(store, bucketId, buffer.toSlice().toByteArray(), expectedValue, 0, STRING_SERDE);
  }

  public static <T> void checkValue(SpillableTimeStateStore store, long bucketId, byte[] bytes,
      T expectedValue, int offset, Serde<T> serde)
  {
    Slice slice = store.getSync(bucketId, new Slice(bytes));

    if (slice == null || slice.length == 0) {
      if (expectedValue != null) {
        Assert.assertEquals(expectedValue, slice);
      } else {
        return;
      }
    }

    T string = serde.deserialize(new Input(slice.buffer, slice.offset + offset, slice.length));

    Assert.assertEquals(expectedValue, string);
  }
}
