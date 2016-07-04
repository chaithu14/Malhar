package org.apache.apex.malhar.lib.utils.serde;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.netlet.util.Slice;

public class SerdeObjectSlice extends KryoSerializableStreamCodec implements Serde<Object,Slice>
{
  @Override
  public Slice serialize(Object object)
  {
    return toByteArray(object);
  }

  @Override
  public Object deserialize(Slice object, MutableInt offset)
  {
    offset.add(object.length);
    return fromByteArray(object);
  }

  @Override
  public Object deserialize(Slice object)
  {
    return fromByteArray(object);
  }
}
