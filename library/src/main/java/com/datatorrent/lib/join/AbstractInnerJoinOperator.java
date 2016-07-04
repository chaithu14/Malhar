package com.datatorrent.lib.join;

import java.util.Arrays;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponent;
import org.apache.apex.malhar.lib.state.spillable.inmem.InMemSpillableComplexComponent;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.BaseOperator;

public abstract class AbstractInnerJoinOperator<K,T> extends BaseOperator
{
  private long expiryTime;
  protected SpillableComplexComponent component;
  protected Spillable.SpillableByteArrayListMultimap<K,T> stream1Data;
  protected Spillable.SpillableByteArrayListMultimap<K,T> stream2Data;
  private boolean isStream1KeyPrimary = false;
  private boolean isStream2KeyPrimary = false;

  @NotNull
  protected List<String> keyFields;
  @NotNull
  protected List<String> timeFields;
  @NotNull
  private String includeFieldStr;
  protected String[][] includeFields;

  protected void processTuple(T tuple, boolean isStream1Data)
  {
    Spillable.SpillableByteArrayListMultimap<K,T> store = isStream1Data ? stream1Data : stream2Data;
    K key = extractKey(tuple,isStream1Data);
    store.put(key, tuple);
    joinStream(key,tuple,isStream1Data);
  }

  protected void joinStream(K key, T tuple, boolean isStream1Data)
  {
    Spillable.SpillableByteArrayListMultimap<K, T> store = isStream1Data ? stream2Data : stream1Data;
    List<T> value = store.get(key);

    // Join the input tuple with the joined tuples
    if (value != null) {
      for (T joinedValue : value) {
        T result = isStream1Data ? joinTuples(Arrays.asList(tuple, joinedValue)) :
            joinTuples(Arrays.asList(joinedValue, tuple));
        if (result != null) {
          emitTuple(result);
        }
      }
    }
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (component == null) {
      createStores();
    }
    component.setup(context);
    String[] streamFields = includeFieldStr.split(";");
    for (int i = 0; i < streamFields.length; i++) {
      includeFields[i] = streamFields[i].split(",");
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    component.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    component.endWindow();
  }

  @Override
  public void teardown()
  {
    component.teardown();
  }

  public abstract K extractKey(T tuple, boolean isStream1Data);

  public abstract T joinTuples(List<T> tuples);

  public abstract void emitTuple(T tuple);

  public void createStores()
  {
    component = new InMemSpillableComplexComponent();
    stream1Data = component.newSpillableByteArrayListMultimap(0,null,null);
    stream2Data = component.newSpillableByteArrayListMultimap(0,null,null);
  }

  public boolean isStream1KeyPrimary()
  {
    return isStream1KeyPrimary;
  }

  public void setStream1KeyPrimary(boolean stream1KeyPrimary)
  {
    isStream1KeyPrimary = stream1KeyPrimary;
  }

  public boolean isStream2KeyPrimary()
  {
    return isStream2KeyPrimary;
  }

  public void setStream2KeyPrimary(boolean stream2KeyPrimary)
  {
    isStream2KeyPrimary = stream2KeyPrimary;
  }

  public String getIncludeFieldStr()
  {
    return includeFieldStr;
  }

  public void setIncludeFieldStr(String includeFieldStr)
  {
    this.includeFieldStr = includeFieldStr;
  }

  public List<String> getKeyFields()
  {
    return keyFields;
  }

  public void setKeyFields(List<String> keyFields)
  {
    this.keyFields = keyFields;
  }

  public List<String> getTimeFields()
  {
    return timeFields;
  }

  public void setTimeFields(List<String> timeFields)
  {
    this.timeFields = timeFields;
  }
}
