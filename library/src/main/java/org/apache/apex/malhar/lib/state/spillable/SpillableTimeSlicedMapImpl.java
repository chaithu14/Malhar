package org.apache.apex.malhar.lib.state.spillable;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.BucketedState;
import org.apache.apex.malhar.lib.state.managed.KeyBucketExtractor;
import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.utils.serde.AffixKeyValueSerdeManager;
import org.apache.apex.malhar.lib.utils.serde.BufferSlice;
import org.apache.apex.malhar.lib.utils.serde.Serde;

import com.esotericsoftware.kryo.io.Input;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

public class SpillableTimeSlicedMapImpl<K,V> implements Spillable.SpillableMap<K, V>, Spillable.SpillableComponent,
    Serializable
{
  private static final long serialVersionUID = 4552547110215784584L;
  private transient WindowBoundedMapCache<K, V> cache = new WindowBoundedMapCache<>();
  private transient Input tmpInput = new Input();

  private TimeExtractor timeExtractor;
  private KeyBucketExtractor<K> keyBucketExtractor;

  @NotNull
  private SpillableTimeStateStore store;

  private long bucket;

  private int size = 0;

  protected AffixKeyValueSerdeManager<K, V> keyValueSerdeManager;

  private SpillableTimeSlicedMapImpl()
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
   * @param serdeValue The {@link Serde} to use when serializing and deserializing values.
   */
  public SpillableTimeSlicedMapImpl(SpillableTimeStateStore store, byte[] identifier, long bucket, Serde<K> serdeKey,
      Serde<V> serdeValue)
  {
    this.store = Preconditions.checkNotNull(store);
    this.bucket = bucket;
    keyValueSerdeManager = new AffixKeyValueSerdeManager<>(null, identifier, Preconditions.checkNotNull(serdeKey), Preconditions.checkNotNull(serdeValue));
  }

  /**
   * Creats a {@link SpillableMapImpl}.
   * @param store The {@link SpillableStateStore} in which to spill to.
   * @param identifier The Id of this {@link SpillableMapImpl}.
   * {@link SpillableMapImpl} in the provided {@link SpillableStateStore}.
   * @param serdeKey The {@link Serde} to use when serializing and deserializing keys.
   * @param serdeValue The {@link Serde} to use when serializing and deserializing values.
   * @param timeExtractor Extract time from the each element and use it to decide where the data goes
   */
  public SpillableTimeSlicedMapImpl(SpillableTimeStateStore store, byte[] identifier, Serde<K> serdeKey,
      Serde<V> serdeValue, @NotNull TimeExtractor<K> timeExtractor, @NotNull KeyBucketExtractor<K> keyBucketExtractor)
  {
    this.store = Preconditions.checkNotNull(store);
    keyValueSerdeManager = new AffixKeyValueSerdeManager<>(null, identifier, Preconditions.checkNotNull(serdeKey), Preconditions.checkNotNull(serdeValue));
    this.timeExtractor = timeExtractor;
    this.keyBucketExtractor = keyBucketExtractor;
  }

  public SpillableTimeSlicedMapImpl(SpillableTimeStateStore store, byte[] identifier, Serde<K> serdeKey,
      Serde<V> serdeValue)
  {
    this.store = Preconditions.checkNotNull(store);
    keyValueSerdeManager = new AffixKeyValueSerdeManager<>(null, identifier, Preconditions.checkNotNull(serdeKey), Preconditions.checkNotNull(serdeValue));
  }

  public SpillableTimeStateStore getStore()
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

    if (cache.getRemovedKeys().contains(key)) {
      return null;
    }

    V val = cache.get(key);

    if (val != null) {
      return val;
    }

    Slice valSlice = store.getSync(getKeyBucket(key), keyValueSerdeManager.serializeDataKey(key, false));

    if (valSlice == null || valSlice == BucketedState.EXPIRED || valSlice.length == 0) {
      return null;
    }

    tmpInput.setBuffer(valSlice.buffer, valSlice.offset, valSlice.length);
    return keyValueSerdeManager.deserializeValue(tmpInput);
  }

  public Future<V> getAsync(Object o)
  {
    K key = (K)o;
    V val = cache.get(key);

    if (val != null) {
      return Futures.immediateFuture(val);
    }

    Future<Slice> valSlice = store.getAsync(getKeyBucket(key), keyValueSerdeManager.serializeDataKey(key, false));
    return new DeserializeValueFuture(valSlice, tmpInput, keyValueSerdeManager);
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
  public Set<Map.Entry<K, V>> entrySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    store.ensureBucket(bucket);
    keyValueSerdeManager.setup(store, bucket);
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    for (K key: cache.getChangedKeys()) {
      store.put(getKeyBucket(key), getTimeBucket(key), keyValueSerdeManager.serializeDataKey(key, true),
          keyValueSerdeManager.serializeValue(cache.get(key)));
    }

    for (K key: cache.getRemovedKeys()) {
      store.put(getKeyBucket(key), getTimeBucket(key), keyValueSerdeManager.serializeDataKey(key, true), BufferSlice.EMPTY_SLICE);
    }
    cache.endWindow();
    keyValueSerdeManager.resetReadBuffer();
  }

  @Override
  public void teardown()
  {
  }

  public TimeExtractor getTimeExtractor()
  {
    return timeExtractor;
  }

  public void setTimeExtractor(TimeExtractor timeExtractor)
  {
    this.timeExtractor = timeExtractor;
  }

  public KeyBucketExtractor<K> getKeyBucketExtractor()
  {
    return keyBucketExtractor;
  }

  public void setKeyBucketExtractor(KeyBucketExtractor<K> keyBucketExtractor)
  {
    this.keyBucketExtractor = keyBucketExtractor;
  }

  private long getTimeBucket(K key)
  {
    return timeExtractor != null ? timeExtractor.getTime(key) : bucket;
  }

  private long getKeyBucket(K key)
  {
    return keyBucketExtractor != null ? keyBucketExtractor.getBucket(key) : bucket;
  }

  public class DeserializeValueFuture implements Future<V>
  {
    private Future<Slice> slice;
    private Input tmpInput;
    private AffixKeyValueSerdeManager<K, V> keyValueSerdeManager;

    public DeserializeValueFuture(Future<Slice> slice, Input tmpInput, AffixKeyValueSerdeManager keyValueSerdeManager)
    {
      this.slice = slice;
      this.tmpInput = tmpInput;
      this.keyValueSerdeManager = keyValueSerdeManager;
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
    public V get() throws InterruptedException, ExecutionException
    {
      Slice valSlice = slice.get();
      if (valSlice == null || valSlice == BucketedState.EXPIRED || valSlice.length == 0) {
        return null;
      }

      tmpInput.setBuffer(valSlice.buffer, valSlice.offset, valSlice.length);
      return keyValueSerdeManager.deserializeValue(tmpInput);
    }

    @Override
    public V get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException
    {
      throw new UnsupportedOperationException();
    }
  }

}
