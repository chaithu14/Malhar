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

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.BucketedState;
import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SliceUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * A Spillable implementation of {@link Map}
 * @param <K> The types of keys.
 * @param <V> The types of values.
 *
 * @since 3.5.0
 */
@DefaultSerializer(FieldSerializer.class)
@InterfaceStability.Evolving
public class SpillableMapImpl<K, V> implements Spillable.SpillableMap<K, V>, Spillable.SpillableComponent,
    Serializable
{
  private transient WindowBoundedMapCache<K, V> cache = new WindowBoundedMapCache<>();
  private transient MutableInt tempOffset = new MutableInt();

  private transient TimeExtractor<K> timeExtractor;

  @NotNull
  private SpillableStateStore store;
  @NotNull
  private byte[] identifier;
  private long bucket;
  @NotNull
  private Serde<K, Slice> serdeKey;
  @NotNull
  private Serde<V, Slice> serdeValue;

  private int size = 0;

  private SpillableMapImpl()
  {
    //for kryo
  }

  /**
   * Creats a {@link SpillableMapImpl}.
   * @param store The {@link SpillableStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableMapImpl}.
   * @param bucket The Id of the bucket used to store this
   * {@link SpillableMapImpl} in the provided {@link SpillableStateStore}.
   * @param serdeKey The {@link Serde} to use when serializing and deserializing keys.
   * @param serdeKey The {@link Serde} to use when serializing and deserializing values.
   */
  public SpillableMapImpl(SpillableStateStore store, byte[] identifier, long bucket, Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue)
  {
    this.store = Preconditions.checkNotNull(store);
    this.identifier = Preconditions.checkNotNull(identifier);
    this.bucket = bucket;
    this.serdeKey = Preconditions.checkNotNull(serdeKey);
    this.serdeValue = Preconditions.checkNotNull(serdeValue);
  }

  public SpillableMapImpl(SpillableStateStore store, byte[] identifier, Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue, TimeExtractor<K> timeExtractor)
  {
    this.store = Preconditions.checkNotNull(store);
    this.identifier = Preconditions.checkNotNull(identifier);
    this.serdeKey = Preconditions.checkNotNull(serdeKey);
    this.serdeValue = Preconditions.checkNotNull(serdeValue);
    this.timeExtractor = timeExtractor;
  }

  public SpillableStateStore getStore()
  {
    return this.store;
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public boolean isEmpty()
  {
    return size == 0;
  }

  @Override
  public boolean containsKey(Object o)
  {
    return get(o) != null;
  }

  @Override
  public boolean containsValue(Object o)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public V get(Object o)
  {
    K key = (K)o;

    long storeBucket = bucket;

    if (cache.getRemovedKeys().contains(key)) {
      return null;
    }

    V val = cache.get(key);

    if (val != null) {
      return val;
    }

    Slice valSlice = store.getSync(storeBucket, SliceUtils.concatenate(identifier, serdeKey.serialize(key)));

    if (valSlice == null || valSlice == BucketedState.EXPIRED || valSlice.length == 0) {
      return null;
    }

    tempOffset.setValue(0);
    //TODO put the get value to cache as well?
    return serdeValue.deserialize(valSlice, tempOffset);
  }

  @Override
  public V put(K k, V v)
  {
    V value = get(k);

    if (value == null) {
      size++;
    }

    cache.put(k, v);

    return value;
  }

  @Override
  public V remove(Object o)
  {
    V value = get(o);

    if (value != null) {
      size--;
    }

    cache.remove((K)o);

    return value;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map)
  {
    for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<K> keySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<V> values()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Entry<K, V>> entrySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    if (timeExtractor != null) {
      timeExtractor.beginWindow(windowId);
    }
  }

  @Override
  public void endWindow()
  {
    for (K key: cache.getChangedKeys()) {
      long actualBucket = timeExtractor != null ? timeExtractor.getTime(key) : bucket;
      store.put(actualBucket, SliceUtils.concatenate(identifier, serdeKey.serialize(key)),
          serdeValue.serialize(cache.get(key)));
    }

    for (K key: cache.getRemovedKeys()) {
      long actualBucket = timeExtractor != null ? timeExtractor.getTime(key) : bucket;
      store.put(actualBucket, SliceUtils.concatenate(identifier, serdeKey.serialize(key)),
          new Slice(ArrayUtils.EMPTY_BYTE_ARRAY));
    }

    if (timeExtractor != null) {
      timeExtractor.endWindow();
    }

    cache.endWindow();
  }

  @Override
  public void teardown()
  {
  }
}
