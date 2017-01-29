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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.spillable.Spillable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.util.concurrent.Futures;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.netlet.util.Slice;

/**
 * Concrete implementation of SpillableListMultimap which is needed for join operator.
 *
 * <b>Properties:</b><br>
 * <b>isKeyContainsMultiValue</b>: Specifies whether the key has multiple value or not. <br>
 * <b>timeBucket</b>: Specifies the lenght of the time bucket.
 *
 *
 * @since 3.5.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class ManagedTimeStateMultiValue<K,V> implements Spillable.SpillableListMultimap<K,V>
{
  private transient StreamCodec streamCodec = null;
  private transient List<Map<Slice, Long>> keyToBucket;
  private boolean isKeyContainsMultiValue = false;
  private long timeBucket;
  @NotNull
  private ManagedTimeStateImpl store;
  private transient Map<Slice, ManagedData> cache;

  public ManagedTimeStateMultiValue()
  {
    if (streamCodec == null) {
      streamCodec = new KryoSerializableStreamCodec();
    }
  }

  public ManagedTimeStateMultiValue(@NotNull ManagedTimeStateImpl store, boolean isKeyContainsMultiValue)
  {
    this();
    this.store = Preconditions.checkNotNull(store);
    this.isKeyContainsMultiValue = isKeyContainsMultiValue;
    cache = new HashMap<>();
    keyToBucket = new ArrayList<>();
    for (int i = 0; i < store.getNumBuckets(); i++) {
      keyToBucket.add(new HashMap<Slice, Long>());
    }
  }

  /**
   * Return the list of values from the store
   * @param k given key
   * @return list of values
   */
  @Override
  public List<V> get(@Nullable K k)
  {
    Slice valueSlice = null;
    Slice keySlice = streamCodec.toByteArray(k);
    ManagedData managedData = cache.get(keySlice);
    long keybucket = getBucketId(k);
    if (managedData != null) {
      valueSlice = managedData.getValue();
    } else {
      Long timeBucket = keyToBucket.get((int)keybucket).get(keySlice);
      if (timeBucket != null) {
        valueSlice = store.getValueFromBucketSync(keybucket, timeBucket, keySlice);
      }
    }
    if (valueSlice == null || valueSlice.length == 0 || valueSlice.buffer == null) {
      return null;
    }
    if (isKeyContainsMultiValue) {
      return (List<V>)streamCodec.fromByteArray(valueSlice);
    }
    List<V> value = new ArrayList<>();
    value.add((V)streamCodec.fromByteArray(valueSlice));
    return  value;
  }

  /**
   * Returns the Future form the store.
   * @param k given key
   * @return
   */
  public CompositeFuture getAsync(@Nullable K k)
  {
    Slice keySlice = streamCodec.toByteArray(k);
    ManagedData managedData = cache.get(keySlice);
    long keyBucket = getBucketId(k);
    if (managedData != null) {
      return new CompositeFuture(Futures.immediateFuture(managedData.getValue()));
    }
    Long timeBucket = keyToBucket.get((int)keyBucket).get(keySlice);
    if (timeBucket != null) {
      return new CompositeFuture(store.getValueFromBucketAsync(keyBucket, timeBucket, keySlice));
    }
    return null;
  }

  @Override
  public Set<K> keySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Multiset<K> keys()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<V> values()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<Map.Entry<K, V>> entries()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<V> removeAll(@Nullable Object o)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear()
  {

  }

  @Override
  public int size()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEmpty()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsKey(@Nullable Object o)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsValue(@Nullable Object o)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsEntry(@Nullable Object o, @Nullable Object o1)
  {
    throw new UnsupportedOperationException();
  }

  /**
   * Inserts the (k,v) into the store.
   * @param k key
   * @param v value
   * @return true if the given (k,v) is successfully inserted into the store otherwise false.
   */
  @Override
  public boolean put(@Nullable K k, @Nullable V v)
  {
    long timeBucketId = store.getTimeBucketAssigner().getTimeBucket(timeBucket);
    if (timeBucketId == -1) {
      return false;
    }
    if (isKeyContainsMultiValue) {
      Slice keySlice = streamCodec.toByteArray(k);
      long bucketId = getBucketId(k);
      Slice valueSlice = store.getSync(bucketId, keySlice);
      List<V> listOb;
      if (valueSlice == null || valueSlice.length == 0) {
        listOb = new ArrayList<>();
      } else {
        listOb = (List<V>)streamCodec.fromByteArray(valueSlice);
      }
      listOb.add(v);
      cache.put(keySlice, new ManagedData((int)bucketId, timeBucket, streamCodec.toByteArray(listOb)));
      //return insertInStore(bucketId, timeBucket, keySlice, streamCodec.toByteArray(listOb));
    } else {
      cache.put(streamCodec.toByteArray(k), new ManagedData((int)getBucketId(k), timeBucket, streamCodec.toByteArray(v)));
    }
    return true;
    //return insertInStore(getBucketId(k), timeBucket, streamCodec.toByteArray(k),streamCodec.toByteArray(v));
  }

  /**
   * Inserts the (k,v) into the store using the specified timebucket.
   * @param k key
   * @param v value
   * @param timeBucket timebucket
   * @return true if the given (k,v) is successfully inserted into the store otherwise false.
   */
  public boolean put(@Nullable K k, @Nullable V v, long timeBucket)
  {
    long timeBucketId = store.getTimeBucketAssigner().getTimeBucket(timeBucket);
    if (timeBucketId == -1) {
      return false;
    }
    Slice keySlice = streamCodec.toByteArray(k);
    long bucketId = getBucketId(k);
    Long oldBucket = keyToBucket.get((int)bucketId).get(keySlice);
    if (oldBucket != null && oldBucket > timeBucketId) {
      return false;
    }
    if (isKeyContainsMultiValue) {
      Slice valueSlice = null;
      if (oldBucket != null) {
        valueSlice = store.getValueFromBucketSync(bucketId, oldBucket, keySlice);
      }
      List<V> listOb;
      if (valueSlice == null || valueSlice.length == 0) {
        listOb = new ArrayList<>();
      } else {
        listOb = (List<V>)streamCodec.fromByteArray(valueSlice);
      }
      listOb.add(v);
      cache.put(keySlice, new ManagedData((int)bucketId, timeBucketId, streamCodec.toByteArray(listOb)));
      //return insertInStore(bucketId, timeBucket, keySlice, streamCodec.toByteArray(listOb));
    } else {
      cache.put(keySlice, new ManagedData((int)getBucketId(k), timeBucketId, streamCodec.toByteArray(v)));
    }
    return true;
    //return insertInStore(getBucketId(k), timeBucket, streamCodec.toByteArray(k),streamCodec.toByteArray(v));
  }

  /**
   * Insert (keySlice,valueSlice) into the store using bucketId and timeBucket.
   * @param bucketId bucket Id
   * @param timeBucket time bucket
   * @param keySlice key slice
   * @param valueSlice value slice
   * @return true if the given (keySlice,valueSlice) is successfully inserted into the
   *         store otherwise false.
   */
  private boolean insertInStore(long bucketId, long timeBucket, Slice keySlice, Slice valueSlice)
  {
    long timeBucketId = store.getTimeBucketAssigner().getTimeBucket(timeBucket);
    if (timeBucketId != -1) {
      store.putInBucket(bucketId, timeBucketId, keySlice, valueSlice);
      return true;
    }
    return false;
  }

  @Override
  public boolean remove(@Nullable Object o, @Nullable Object o1)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean putAll(@Nullable K k, Iterable<? extends V> iterable)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean putAll(Multimap<? extends K, ? extends V> multimap)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<V> replaceValues(K k, Iterable<? extends V> iterable)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<K, Collection<V>> asMap()
  {
    throw new UnsupportedOperationException();
  }

  public long getBucketId(K k)
  {
    return k.hashCode() % store.getNumBuckets();
  }

  public long getTimeBucket()
  {
    return timeBucket;
  }

  public void setTimeBucket(long timeBucket)
  {
    this.timeBucket = timeBucket;
  }

  public StreamCodec getStreamCodec()
  {
    return streamCodec;
  }

  public void setStreamCodec(StreamCodec streamCodec)
  {
    this.streamCodec = streamCodec;
  }

  public void endWindow()
  {
    for (Slice key: cache.keySet()) {
      ManagedData managedData = cache.get(key);
      keyToBucket.get(managedData.getKeyBucket()).put(key, managedData.getTimeBucket());
      store.putInBucket(managedData.getKeyBucket(), managedData.getTimeBucket(), key, managedData.getValue());
    }
    cache.clear();
  }

  public class CompositeFuture implements Future<List>
  {
    public Future<Slice> slice;

    public CompositeFuture(Future<Slice> slice)
    {
      this.slice = slice;
    }

    @Override
    public boolean cancel(boolean b)
    {
      return slice.cancel(b);
    }

    @Override
    public boolean isCancelled()
    {
      return slice.isCancelled();
    }

    @Override
    public boolean isDone()
    {
      return slice.isDone();
    }

    /**
     * Converts the single element into the list.
     * @return the list of values
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Override
    public List get() throws InterruptedException, ExecutionException
    {
      List<V> value = null;
      Slice valueSlice = slice.get();
      if (valueSlice == null || valueSlice.length == 0 || valueSlice.buffer == null) {
        return null;
      }
      if (isKeyContainsMultiValue) {
        value = (List<V>)streamCodec.fromByteArray(valueSlice);
      }  else {
        value = new ArrayList<>();
        value.add((V)streamCodec.fromByteArray(valueSlice));
      }
      return value;
    }

    @Override
    public List get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException
    {
      throw new UnsupportedOperationException();
    }
  }

  public static class ManagedData
  {
    private int keyBucket;
    private long timeBucket;
    private Slice value;

    public ManagedData(int keyBucket, long timeBucket, Slice value)
    {
      this.keyBucket = keyBucket;
      this.timeBucket = timeBucket;
      this.value = value;
    }

    public int getKeyBucket()
    {
      return keyBucket;
    }

    public void setKeyBucket(int keyBucket)
    {
      this.keyBucket = keyBucket;
    }

    public long getTimeBucket()
    {
      return timeBucket;
    }

    public void setTimeBucket(long timeBucket)
    {
      this.timeBucket = timeBucket;
    }

    public Slice getValue()
    {
      return value;
    }

    public void setValue(Slice value)
    {
      this.value = value;
    }
  }

}
