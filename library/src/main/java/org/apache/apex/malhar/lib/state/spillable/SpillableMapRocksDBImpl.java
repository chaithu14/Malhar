package org.apache.apex.malhar.lib.state.spillable;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.apex.malhar.lib.state.BucketedState;
import org.apache.apex.malhar.lib.state.spillable.managed.RocksDBStateStore;
import com.datatorrent.api.Context;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.netlet.util.Slice;

public class SpillableMapRocksDBImpl<K, V> implements Spillable.SpillableMap<K, V>, Spillable.SpillableComponent,
    Serializable
{
  private transient RocksDBStateStore store;
  private KryoSerializableStreamCodec kryo = new KryoSerializableStreamCodec();
  private transient WindowBoundedMapCache<K, V> cache = new WindowBoundedMapCache<>();

  public SpillableMapRocksDBImpl(RocksDBStateStore store)
  {
    this.store = store;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    store.setup(context);
  }

  @Override
  public void teardown()
  {
    store.teardown();
  }

  @Override
  public int size()
  {
    return 0;
  }

  @Override
  public boolean isEmpty()
  {
    return false;
  }

  @Override
  public boolean containsKey(Object key)
  {
    return false;
  }

  @Override
  public boolean containsValue(Object value)
  {
    return false;
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

    Slice valSlice = store.getSync(0, kryo.toByteArray(key));

    if (valSlice == null || valSlice == BucketedState.EXPIRED || valSlice.length == 0) {
      return null;
    }

    return (V)kryo.fromByteArray(valSlice);
  }

  @Override
  public V put(K k, V v)
  {
    V value = get(k);

    cache.put(k, v);

    return value;
  }

  @Override
  public V remove(Object o)
  {
    V value = get(o);

    cache.remove((K)o);

    return value;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m)
  {
    for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear()
  {

  }

  @Override
  public Set<K> keySet()
  {
    return null;
  }

  @Override
  public Collection<V> values()
  {
    return null;
  }

  @Override
  public Set<Entry<K, V>> entrySet()
  {
    return null;
  }

  @Override
  public void beginWindow(long windowId)
  {
    store.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    for (K key: cache.getChangedKeys()) {
      store.put(0, kryo.toByteArray(key), kryo.toByteArray(cache.get(key)));
    }

    for (K key: cache.getRemovedKeys()) {
    }
    cache.endWindow();
  }
}
