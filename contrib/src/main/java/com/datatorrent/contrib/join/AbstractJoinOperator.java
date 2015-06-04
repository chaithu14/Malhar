package com.datatorrent.contrib.join;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.lib.bucket.Event;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.LoggerFactory;

public abstract class AbstractJoinOperator<T> extends BaseOperator implements Operator.CheckpointListener, Operator.ActivationListener<Context.OperatorContext>
{
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(AbstractJoinOperator.class);
  private BackupStore store[] = (BackupStore[]) Array.newInstance(BackupStore.class, 2);

  private long expiryTime;

  private String keyFields;

  protected String[][] includeFields;

  private String includeFieldStr;

  protected int bucketSpanInMillis = 30000;
  private String[] keys;

  public final transient DefaultOutputPort<List<T>> outputPort = new DefaultOutputPort<List<T>>();

  private transient long currentWId;

  @Override
  public void setup(Context.OperatorContext context)
  {
    String appPath = context.getValue(DAG.APPLICATION_PATH);
    keys = keyFields.split(",");
    String path = appPath + "/" + "buckets/"+context.getId();
    store[0] = new InMemoryStore<TimeEvent>(expiryTime, bucketSpanInMillis, path + "/UP");
    store[1] = new InMemoryStore<TimeEvent>(expiryTime, bucketSpanInMillis, path + "/DOWN");

    store[0].setup(context);
    store[1].setup(context);
    populateIncludeFields();
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWId = windowId;
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

  private static long readLong(byte[] bytes, int offset)
  {
    long r = 0;
    for (int i = offset; i < offset + 8 & i < bytes.length ; i++) {
      r = r << 8;
      r += bytes[i];
    }
    return r;
  }


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
    store[0].endWindow(currentWId);
    store[1].endWindow(currentWId);
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

  @Override public void deactivate()
  {
    store[0].shutdown();
    store[1].shutdown();
  }

  @Override
  public void activate(Context.OperatorContext ctx)
  {
    logger.info("Activate the Join Operator");
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
