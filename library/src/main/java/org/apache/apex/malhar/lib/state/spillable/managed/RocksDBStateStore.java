package org.apache.apex.malhar.lib.state.spillable.managed;

import java.io.Serializable;
import java.util.concurrent.Future;

import javax.validation.constraints.NotNull;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import org.apache.apex.malhar.lib.state.managed.Bucket;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

@DefaultSerializer(JavaSerializer.class)
public class RocksDBStateStore implements SpillableStateStore, Serializable
{
  protected String dbPath;
  protected transient RocksDB db;

  @Override
  public void setup(Context.OperatorContext context)
  {
    final Options options = new Options().setCreateIfMissing(true);
    try {
      db = RocksDB.open(options, dbPath);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown()
  {
    db.close();
  }

  @Override
  public void beforeCheckpoint(long l)
  {

  }

  @Override
  public void checkpointed(long l)
  {

  }

  @Override
  public void committed(long l)
  {

  }

  @Override
  public Bucket getBucket(long bucketId)
  {
    return null;
  }

  @Override
  public Bucket ensureBucket(long bucketId)
  {
    return null;
  }

  @Override
  public void beginWindow(long windowId)
  {

  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void put(long bucketId, @NotNull Slice key, @NotNull Slice value)
  {
    try {
      db.put(key.buffer, value.buffer);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Slice getSync(long bucketId, @NotNull Slice key)
  {
    try {
      return new Slice(db.get(key.buffer));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Future<Slice> getAsync(long bucketId, @NotNull Slice key)
  {
    return null;
  }

  public String getDbPath()
  {
    return dbPath;
  }

  public void setDbPath(String dbPath)
  {
    this.dbPath = dbPath;
  }
}
