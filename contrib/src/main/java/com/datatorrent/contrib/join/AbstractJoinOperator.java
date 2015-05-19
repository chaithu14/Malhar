package com.datatorrent.contrib.join;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.lib.bucket.Event;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.tuple.MutablePair;
import org.slf4j.LoggerFactory;

public abstract class AbstractJoinOperator<T> extends BaseOperator implements Operator.ActivationListener<Context.OperatorContext>, Operator.CheckpointListener
{
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(AbstractJoinOperator.class);
  private transient InMemoryStore lStore;
  private transient InMemoryStore rStore;

  private long expiryTime = 1;

  private String keyFields;

  private transient ArrayBlockingQueue<MutablePair<TimeEvent,Boolean>> holdingBuffer;

  protected String[][] includeFields;

  private String includeFieldStr;

  private int bufferSize = 1024;

  private transient Boolean isAlive = false;

  protected int bucketSpanInMillis = 30000;
  private transient long sleepTimeMillis = 100;

  public final transient DefaultOutputPort<List<T>> outputPort = new DefaultOutputPort<List<T>>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    isAlive = true;
    String[] keys = keyFields.split(",");
    lStore = new InMemoryStore<TimeEvent>(keys[0], expiryTime, bucketSpanInMillis);
    rStore = new InMemoryStore<TimeEvent>(keys[1], expiryTime, bucketSpanInMillis);
    holdingBuffer = new ArrayBlockingQueue<MutablePair<TimeEvent, Boolean>>(bufferSize);

    lStore.setup();
    rStore.setup();
    populateIncludeFields();
    logger.info("Setup: {}", holdingBuffer.size());
  }

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<T> input1 = new DefaultInputPort<T>()
  {
    @Override public void process(T tuple)
    {
      TimeEvent t = new TimeEvent(getValue(lStore.keyField, tuple), System.currentTimeMillis(), tuple);
      lStore.put(t);
      joinedTuple(t, true);
      /*try {
        TimeEvent t = new TimeEvent(getValue(lStore.keyField, tuple), System.currentTimeMillis(), tuple);
        holdingBuffer.put(new MutablePair<TimeEvent, Boolean>(t, true));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }*/
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<T> input2 = new DefaultInputPort<T>()
  {
    @Override public void process(T tuple)
    {
      TimeEvent t = new TimeEvent(getValue(rStore.keyField, tuple), System.currentTimeMillis(), tuple);
      rStore.put(t);
      joinedTuple(t, false);
      /*try {
        TimeEvent t = new TimeEvent(getValue(rStore.keyField, tuple), System.currentTimeMillis(), tuple);
        holdingBuffer.put(new MutablePair<TimeEvent, Boolean>(t, false));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }*/
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

  private void startThread()
  {
    ExecutorService threadExecutor = Executors.newFixedThreadPool(1);
    threadExecutor.submit(new Runnable() {
      @Override
      public void run()
      {
        while (isAlive) {
          int count = holdingBuffer.size();
          for(int i = 0; i < count; i++) {
            MutablePair<TimeEvent,Boolean> tuple = holdingBuffer.poll();
            if(tuple.getRight()) {
              joinedTuple(tuple.getLeft(), tuple.getRight());
            } else {
              joinedTuple(tuple.getLeft(), tuple.getRight());
            }
          }
        }
      }
    });
  }

  private void joinedTuple(TimeEvent tuple, Boolean isFirst)
  {
    //Object key = firstStore.getKey(getObject(tuple));
    Object key = tuple.getEventKey();
    Object value ;
    if(isFirst) {
      value = rStore.getValidTuples(key, tuple);
      //lStore.put(tuple);
    } else {
      value = lStore.getValidTuples(key,tuple);
      //rStore.put(tuple);
    }
    if(value != null) {
      ArrayList<Event> joinedValues = (ArrayList<Event>)value;
      List<T> result = new ArrayList<T>();
      for(int idx = 0; idx < joinedValues.size(); idx++) {
        T output = createOutputTuple();
        Object jTuple = tuple.getTuple();
        if(!isFirst) {
          jTuple = ((TimeEvent)joinedValues.get(idx)).getTuple();
        }
        addValue(output, jTuple, true);
        if(isFirst) {
          jTuple = ((TimeEvent)joinedValues.get(idx)).getTuple();
        } else {
          jTuple = tuple.getTuple();
        }
        addValue(output, jTuple, false);
        result.add(output);
      }
      if(result.size() != 0) {
        outputPort.emit(result);
      }
    }

  }

  @Override
  public void endWindow()
  {
    logger.info("ENdWindow: {}", holdingBuffer.size());
    handleIdleTime();
    logger.info("After handle Time: ENdWindow: {}", holdingBuffer.size());
  }

  public void handleIdleTime()
  {
    if(holdingBuffer.size() != 0) {
      try {
        Thread.sleep(sleepTimeMillis);
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
  }
  @Override
  public void checkpointed(long windowId)
  {
    lStore.checkpointed(windowId);
    rStore.checkpointed(windowId);
  }

  @Override
  public void committed(long windowId)
  {
    lStore.committed(windowId);
    lStore.committed(windowId);
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

  @Override public void activate(Context.OperatorContext operatorContext)
  {
    isAlive = true;
    //startThread();
  }

  @Override public void deactivate()
  {
    isAlive = false;
    lStore.shutdown();
    rStore.shutdown();
  }

  protected abstract T createOutputTuple();

  protected abstract void addValue(T output, Object extractTuple, Boolean isFirst);

  public abstract Object getValue(String keyField, Object tuple);

  public long getExpiryTime()
  {
    return expiryTime;
  }

  public void setBucketSpanInMillis(int bucketSpanInMillis)
  {
    this.bucketSpanInMillis = bucketSpanInMillis;
  }
}
