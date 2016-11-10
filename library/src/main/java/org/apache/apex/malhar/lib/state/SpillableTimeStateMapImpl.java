package org.apache.apex.malhar.lib.state;

import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.state.spillable.SpillableMapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.AffixKeyValueSerdeManager;
import org.apache.apex.malhar.lib.utils.serde.BufferSlice;
import org.apache.apex.malhar.lib.utils.serde.Serde;

import com.google.common.base.Preconditions;

import com.datatorrent.netlet.util.Slice;

public class SpillableTimeStateMapImpl<K,V> extends SpillableMapImpl<K,V>
{
  public SpillableTimeStateMapImpl(SpillableStateStore store, byte[] identifier, Serde<K> serdeKey,
    Serde<V> serdeValue, TimeExtractor<K> timeExtractor)
  {
    this.store = Preconditions.checkNotNull(store);
    keyValueSerdeManager = new AffixKeyValueSerdeManager<>(null, identifier, Preconditions.checkNotNull(serdeKey), Preconditions.checkNotNull(serdeValue));
    this.timeExtractor = timeExtractor;
  }

  @Override
  public void endWindow()
  {
    for (K key: cache.getChangedKeys()) {
      store.put(0, getBucket(key), keyValueSerdeManager.serializeDataKey(key, true),
        keyValueSerdeManager.serializeValue(cache.get(key)));
    }

    for (K key: cache.getRemovedKeys()) {
      store.put(getBucket(key), getBucket(key), keyValueSerdeManager.serializeDataKey(key, true), BufferSlice.EMPTY_SLICE);
    }
    cache.endWindow();
    keyValueSerdeManager.resetReadBuffer();
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

    Slice valSlice = store.getSync(0, keyValueSerdeManager.serializeDataKey(key, false));

    if (valSlice == null || valSlice == BucketedState.EXPIRED || valSlice.length == 0) {
      return null;
    }

    tmpInput.setBuffer(valSlice.buffer, valSlice.offset, valSlice.length);
    return keyValueSerdeManager.deserializeValue(tmpInput);
  }
}
