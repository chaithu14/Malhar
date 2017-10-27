package org.apache.apex.malhar.lib.state.spillable.managed;

import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import com.datatorrent.api.Context;

@DefaultSerializer(JavaSerializer.class)
public class RocksDBTTLStateStore extends RocksDBStateStore
{
  private int expiryInSeconds;

  @Override
  public void setup(Context.OperatorContext context)
  {
    final Options options = new Options().setCreateIfMissing(true);
    try {
      db = TtlDB.open(options, dbPath, expiryInSeconds, false);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public int getExpiryInSeconds()
  {
    return expiryInSeconds;
  }

  public void setExpiryInSeconds(int expiryInSeconds)
  {
    this.expiryInSeconds = expiryInSeconds;
  }
}
