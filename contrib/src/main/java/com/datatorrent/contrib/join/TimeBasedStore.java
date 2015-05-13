package com.datatorrent.contrib.join;

import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.HDHTStore;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.MutablePair;

public class TimeBasedStore extends BackupStore
{
  private long expiryTime;

  private HDHTStore store;

  private final long BUCKET = 1L;

  public TimeBasedStore(String keyField, long expiryTime, AbstractJoinOperator op)
  {
    super(keyField, op);
    this.expiryTime = expiryTime;
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath("operator/"+keyField);
    store = new HDHTStore();
    store.setFileStore(hdsFile);
    store.setMaxFileSize(1); // limit to single entry per file
    store.setFlushSize(0); // flush after every key

    store.writeExecutor = MoreExecutors.sameThreadExecutor(); // synchronous flush on endWindow
  }

  @Override public void setup()
  {
    store.setup(null);
  }

  @Override public Object getValidTuples(Object key, Object tuple)
  {
    byte[] key2bytes = key.toString().getBytes();
    Slice keySlice = new Slice(key2bytes, 0, key2bytes.length);
    byte[] value =  store.getUncommitted(BUCKET, keySlice);
    Long startTime = ((MutablePair<Object,Long>)tuple).getRight();
    if(value != null ) {
      Input lInput = new Input(value);
      Kryo kryo = new Kryo();
      List<Object> t = (List<Object>)kryo.readObject(lInput, ArrayList.class);
      List<Object> validTuples = new ArrayList<Object>();
      for(Object rightTuple: t) {
        if(Math.abs(startTime - ((MutablePair<Object,Long>)rightTuple).getRight()) <= expiryTime) {
          validTuples.add(rightTuple);
        }
      }
      return validTuples;
    }
    return null;
  }

  @Override public void committed(long windowId)
  {
    store.committed(windowId);
  }

  @Override public void checkpointed(long windowId)
  {
    store.checkpointed(windowId);
  }

  @Override public void put(Object tuple)
  {
    Object key = getKey(op.getObject(tuple));
    byte[] key2bytes = key.toString().getBytes();
    Slice keySlice = new Slice(key2bytes, 0, key2bytes.length);
    Kryo kryo = new Kryo();
    ByteArrayOutputStream bos = null;
    byte[] value = store.getUncommitted(BUCKET, keySlice);
    if(value == null) {
      List<Object> ob = new ArrayList<Object>();
      ob.add(tuple);

      bos = new ByteArrayOutputStream();
      Output output = new Output(bos);
      kryo.writeObject(output, ob);
      output.close();
    } else {
      Input lInput = new Input(value);
      List<Object> t = (List<Object>)kryo.readObject(lInput, ArrayList.class);
      t.add(tuple);
      bos = new ByteArrayOutputStream();
      Output output = new Output(bos);
      kryo.writeObject(output, t);
      output.close();
    }
    try {
      store.put(BUCKET, keySlice, bos.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
