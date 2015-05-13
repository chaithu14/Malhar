package com.datatorrent.contrib.join;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.eclipse.jetty.util.ArrayQueue;

public class CountBasedStore extends BackupStore
{
  private Queue<Object> store;
  private Map<Object, List<Object>> keyBasedStore;

  private int windowSize;

  public CountBasedStore(String keyField, int windowSize, AbstractJoinOperator op)
  {
    super(keyField, op);
    this.windowSize = windowSize;
    store = new ArrayQueue<Object>(windowSize);
    keyBasedStore = new HashMap<Object, List<Object>>();
  }

  @Override public Object getValidTuples(Object key, Object tuple)
  {
    return keyBasedStore.get(key);
  }

  @Override public void put(Object tuple)
  {

    Object[] parameters = new Object[2];
    parameters[0] = keyField;
    if(store.size() == windowSize) {
      Object pollTuple = store.poll();

      Object pollkey = getKey(pollTuple);
      keyBasedStore.get(pollkey).remove(pollTuple);
      if(keyBasedStore.get(pollkey).size() == 0) {
        keyBasedStore.remove(pollkey);
      }
    }
    store.add(tuple);
    Object key = getKey(tuple);
    if(keyBasedStore.containsKey(key)) {
      keyBasedStore.get(key).add(tuple);
    } else {
      keyBasedStore.put(key, new ArrayList<Object>(Collections.singletonList(tuple)));
    }
    /*logger.info("Size: {} - > {}", store.size(), store);
    logger.info("Size: {} - > {}", keyBasedStore.size(), keyBasedStore);*/
  }
}
