package com.datatorrent.contrib.join;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.lib.bucket.Event;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.LoggerFactory;

public abstract class AbstractJoinOperator<T> extends BaseOperator implements Operator.CheckpointListener
{
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(AbstractJoinOperator.class);
  private transient BackupStore store[] = (BackupStore[]) Array.newInstance(BackupStore.class, 2);

  private long expiryTime;

  private String keyFields;

  protected String[][] includeFields;

  private String includeFieldStr;

  protected int bucketSpanInMillis = 30000;
  private String[] keys;

  public final transient DefaultOutputPort<List<T>> outputPort = new DefaultOutputPort<List<T>>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    keys = keyFields.split(",");
    store[0] = new InMemoryStore<TimeEvent>(expiryTime, bucketSpanInMillis);
    store[1] = new InMemoryStore<TimeEvent>(expiryTime, bucketSpanInMillis);

    store[0].setup();
    store[1].setup();
    populateIncludeFields();
  }

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<T> input1 = new DefaultInputPort<T>()
  {
    @Override public void process(T tuple)
    {
      TimeEvent t = createEvent(keys[0], tuple);
      store[0].put(t);
      join(t, true);
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<T> input2 = new DefaultInputPort<T>()
  {
    @Override public void process(T tuple)
    {
      TimeEvent t = createEvent(keys[1], tuple);
      store[1].put(t);
      join(t, false);
    }
  };

  private void populateIncludeFields()
  {
    includeFields = new String[2][];
    String[] portFields = includeFieldStr.split(";");
    for(int i = 0; i < portFields.length; i++) {
      includeFields[i] = portFields[i].split(",");
    }
  }

  /**
   * Get the tuples from another store based on join constraint and key
   * @param tuple input
   * @param isFirst whether the given tuple is from first port or not
   */
  private void join(Object tuple, Boolean isFirst)
  {
    // Get the valid tuples from the store based on key
    Object value ;
    if(isFirst) {
      value = store[1].getValidTuples(tuple);
    } else {
      value = store[0].getValidTuples(tuple);
    }
    // Join the input tuple with the joined tuples
    if(value != null) {
      ArrayList<Event> joinedValues = (ArrayList<Event>)value;
      List<T> result = new ArrayList<T>();
      for(int idx = 0; idx < joinedValues.size(); idx++) {
        T output = createOutputTuple();
        addValue(output, ((TimeEvent)tuple).getValue(), isFirst);
        addValue(output, ((TimeEvent)joinedValues.get(idx)).getValue(), !isFirst);
        result.add(output);
      }
      if(result.size() != 0) {
        outputPort.emit(result);
      }
    }
  }

  /**
   * Create the output object
   * @return
   */
  protected abstract T createOutputTuple();

  /**
   * Get the values from extractTuple and set these values to the output
   * @param output
   * @param extractTuple
   * @param isFirst
   */
  protected abstract void addValue(T output, Object extractTuple, Boolean isFirst);

  /**
   * Get the value of the field from the given tuple
   * @param keyField
   * @param tuple
   * @return
   */
  protected abstract Object getValue(String keyField, Object tuple);

  @Override
  public void endWindow()
  {
  }

  @Override
  public void checkpointed(long windowId)
  {
    store[0].checkpointed(windowId);
    store[1].checkpointed(windowId);
  }

  @Override
  public void committed(long windowId)
  {
    store[0].committed(windowId);
    store[1].committed(windowId);
  }

  public void setExpiryTime(long expiryTime)
  {
    this.expiryTime = expiryTime;
  }

  public void setKeyFields(String keyFields)
  {
    this.keyFields = keyFields;
  }

  public void setIncludeFieldStr(String includeFieldStr)
  {
    this.includeFieldStr = includeFieldStr;
  }

  public TimeEvent createEvent(String keyField, Object tuple)
  {
    return new TimeEvent(getValue(keyField, tuple), System.currentTimeMillis(), tuple);
  }

  public long getExpiryTime()
  {
    return expiryTime;
  }

  public void setBucketSpanInMillis(int bucketSpanInMillis)
  {
    this.bucketSpanInMillis = bucketSpanInMillis;
  }
}
