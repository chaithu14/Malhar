package com.datatorrent.contrib.join;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.tuple.MutablePair;
import org.slf4j.LoggerFactory;

public abstract class AbstractJoinOperator<T> extends BaseOperator implements Operator.ActivationListener<Context.OperatorContext>, Operator.CheckpointListener
{
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(AbstractJoinOperator.class);
  private BackupStore lStore;
  private BackupStore rStore;

  private boolean isTimeBased = false;

  private long expiryTime;

  private int windowSize;

  private String keyFields;

  private ArrayBlockingQueue<MutablePair<Object,Boolean>> holdingBuffer;

  private List<String> includeFields;

  private String includeFieldStr;

  private int bufferSize = 1024;

  private transient Boolean isAlive = false;



  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    isAlive = true;
    String[] keys = keyFields.split(",");
    if(lStore == null && rStore == null)
    {
      if(isTimeBased) {
        lStore = new TimeBasedStore(keys[0], expiryTime, this);
        rStore = new TimeBasedStore(keys[1], expiryTime, this);
      } else {
        lStore = new CountBasedStore(keys[0], windowSize, this);
        rStore = new CountBasedStore(keys[1], windowSize, this);
      }
      holdingBuffer = new ArrayBlockingQueue<MutablePair<Object, Boolean>>(bufferSize);
    }

    lStore.setup();
    rStore.setup();
    populateIncludeFields();
  }

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<T> input1 = new DefaultInputPort<T>()
  {
    @Override public void process(T tuple)
    {
      try {
        if(isTimeBased) {
          Object t = new MutablePair<Object, Long>(tuple, System.currentTimeMillis());
          holdingBuffer.put(new MutablePair<Object, Boolean>(t, true));
        } else {
          holdingBuffer.put(new MutablePair<Object, Boolean>(tuple, true));
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<T> input2 = new DefaultInputPort<T>()
  {
    @Override public void process(T tuple)
    {
      try {
        if(isTimeBased) {
          Object t = new MutablePair<Object, Long>(tuple, System.currentTimeMillis());
          holdingBuffer.put(new MutablePair<Object, Boolean>(t, false));
        } else {
          holdingBuffer.put(new MutablePair<Object, Boolean>(tuple, false));
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  };

  private void populateIncludeFields()
  {
    includeFields = new ArrayList<String>();
    for(String f: includeFieldStr.split(";")) {
      includeFields.addAll(Arrays.asList(f.split(",")));
      includeFields.add(null);
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
            MutablePair<Object,Boolean> tuple = holdingBuffer.poll();
            if(tuple.getRight()) {
              joinedTuple(tuple.getLeft(), lStore, rStore, tuple.getRight());
            } else {
              joinedTuple(tuple.getLeft(), rStore, lStore, tuple.getRight());
            }
          }
        }
      }
    });
  }

  private void joinedTuple(Object tuple, BackupStore firstStore, BackupStore secondStore, Boolean isFirst)
  {
    Object key = firstStore.getKey(getObject(tuple));
    Object value = secondStore.getValidTuples(key, tuple);
    if(value != null) {
      ArrayList<Object> joinedValues = (ArrayList<Object>)value;
      for(int idx = 0; idx < joinedValues.size(); idx++) {
        T output = createOutputTuple();
        Object jTuple = tuple;
        if(!isFirst) {
          jTuple = joinedValues.get(idx);
        }
        for(String f: includeFields) {
          if(f == null) {
            if(isFirst) {
              jTuple = joinedValues.get(idx);
            } else {
              jTuple = tuple;
            }
            continue;
          }
          addValue(output, f, getObject(jTuple));
        }
        outputPort.emit(output);
      }
    }
    firstStore.put(tuple);
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
  public void setTimeBased(boolean isTimeBased)
  {
    this.isTimeBased = isTimeBased;
  }

  public void setExpiryTime(long expiryTime)
  {
    this.expiryTime = expiryTime;
  }

  public void setWindowSize(int windowSize)
  {
    this.windowSize = windowSize;
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
    startThread();
  }

  @Override public void deactivate()
  {
    isAlive = false;
  }

  protected abstract T createOutputTuple();

  protected abstract void addValue(T output, String field, Object extractTuple);

  public abstract Object getValue(String keyField, Object tuple);

  public Object getObject(Object tuple) {
    if(isTimeBased) {
      return ((MutablePair<Object,Long>)tuple).getLeft();
    }
    return tuple;
  }

}
