package com.datatorrent.lib.join;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.apex.malhar.lib.state.managed.ManagedStateImpl;
import org.apache.apex.malhar.lib.state.spillable.Spillable;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.netlet.util.Slice;

public class ManagedStateMultiMap extends ManagedStateImpl implements Spillable.SpillableByteArrayListMultimap<Object,Object>
{
  private boolean isKeyContainsMultiValue = false;
  private KryoSerializableStreamCodec obSlice = new KryoSerializableStreamCodec();

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
  public boolean containsKey(@Nullable Object o)
  {
    return false;
  }

  @Override
  public boolean containsValue(@Nullable Object o)
  {
    return false;
  }

  @Override
  public boolean containsEntry(@Nullable Object o, @Nullable Object o1)
  {
    return false;
  }

  @Override
  public boolean put(Object key, Object value)
  {
    if (isKeyContainsMultiValue) {
      Slice keySlice = obSlice.toByteArray(key);
      Slice valueSlice = super.getSync(key.hashCode(), keySlice);
      List<Object> listOb = (List<Object>)obSlice.fromByteArray(valueSlice);
      listOb.add(value);
      super.put(key.hashCode(), keySlice, obSlice.toByteArray(listOb));
    } else {
      super.put(key.hashCode(), obSlice.toByteArray(key),obSlice.toByteArray(value));
    }
    return true;
  }

  @Override
  public List get(@Nullable Object tuple)
  {
    List<Object> validTuples = null;
    Slice value = super.getSync(tuple.hashCode(), obSlice.toByteArray(tuple));
    if (isKeyContainsMultiValue) {
      validTuples = (List<Object>)obSlice.fromByteArray(value);
    }  else {
      if (value == null || value.length == 0 || value.buffer == null) {
        return null;
      }
      validTuples = new ArrayList<>();
      validTuples.add(obSlice.fromByteArray(value));
    }
    return  validTuples;
  }


  @Override
  public boolean remove(@Nullable Object o, @Nullable Object o1)
  {
    return false;
  }

  @Override
  public boolean putAll(@Nullable Object o, Iterable iterable)
  {
    return false;
  }

  @Override
  public boolean putAll(Multimap multimap)
  {
    return false;
  }

  public boolean isKeyContainsMultiValue()
  {
    return isKeyContainsMultiValue;
  }

  public void setKeyContainsMultiValue(boolean keyContainsMultiValue)
  {
    isKeyContainsMultiValue = keyContainsMultiValue;
  }

  @Override
  public Set keySet()
  {
    return null;
  }

  @Override
  public Multiset keys()
  {
    return null;
  }

  @Override
  public Collection values()
  {
    return null;
  }

  @Override
  public Collection<Map.Entry<Object, Object>> entries()
  {
    return null;
  }

  @Override
  public List removeAll(@Nullable Object o)
  {
    return null;
  }

  @Override
  public void clear()
  {

  }

  @Override
  public List replaceValues(Object o, Iterable iterable)
  {
    return null;
  }

  @Override
  public Map asMap()
  {
    return null;
  }
}
