/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.join;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * <p>
 * This is the base implementation of join operator. Operator receives tuples from multiple streams,
 * applies the join operation based on constraint and emit the joined value.
 * Subclasses should provide implementation to createOutputTuple,copyValue, getKeyValue, getTime methods.
 *
 * <b>Properties:</b><br>
 * <b>expiryTime</b>: Expiry time for stored tuples<br>
 * <b>includeFieldStr</b>: List of comma separated fields to be added to the output tuple. Ex: Field1,Field2;Field3,Field4<br>
 * <b>keyFields</b>: List of comma separated key field for both the streams. Ex: Field1,Field2<br>
 * <b>timeFields</b>: List of comma separated time field for both the streams. Ex: Field1,Field2<br>
 * <b>bucketSpanInMillis</b>: Span of each bucket in milliseconds.<br>
 * <b>strategy</b>: Type of join operation. Default type is inner join<br>
 * <br>
 * </p>
 *
 * @displayName Abstract Join Operator
 * @tags join
 * @since 2.2.0
 */

public abstract class AbstractJoinOperator<T> extends BaseOperator implements Operator.CheckpointListener
{
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(AbstractJoinOperator.class);
  public final transient DefaultOutputPort<List<T>> outputPort = new DefaultOutputPort<List<T>>();
  protected String[] timeFields;

  protected String[][] includeFields;
  // Fields to compare from both the streams
  protected String[] keys;
  // Strategy of Join operation, by default the option is inner join
  protected JoinStrategy strategy = JoinStrategy.INNER_JOIN;
  // This represents whether the processing tuple is from left port or not
  protected Boolean isLeft;
  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<T> input1 = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      isLeft = true;
      processTuple(tuple);
    }
  };
  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<T> input2 = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      isLeft = false;
      processTuple(tuple);
    }
  };
  // Stores for each of the input port
  private BackupStore store[] = (BackupStore[])Array.newInstance(BackupStore.class, 2);
  private String includeFieldStr;
  private String keyFieldStr;
  private String timeFieldStr;

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (store[0] == null) {
      throw new RuntimeException("Left Store is Empty");
    }
    if (store[1] == null) {
      throw new RuntimeException("Right Store is Empty");
    }
    // Checks whether the strategy is outer join and set it to store
    boolean isOuter = strategy.equals(JoinStrategy.LEFT_OUTER_JOIN) || strategy.equals(JoinStrategy.OUTER_JOIN);
    store[0].isOuterJoin(isOuter);
    isOuter = strategy.equals(JoinStrategy.RIGHT_OUTER_JOIN) || strategy.equals(JoinStrategy.OUTER_JOIN);
    store[1].isOuterJoin(isOuter);
    // Setup the stores
    store[0].setup();
    store[1].setup();

    populateFields();
  }

  /**
   * Create the event with the given tuple. If it successfully inserted it into the store
   * then it does the join operation
   *
   * @param tuple
   */
  protected void processTuple(T tuple)
  {
    int idx = 0;
    if (!isLeft) {
      idx = 1;
    }
    TimeEvent t = createEvent(tuple);
    if (store[idx].put(t)) {
      join(t, isLeft);
    }
  }

  private void populateFields()
  {
    populateIncludeFields();
    populateKeyFields();
    if (timeFieldStr != null) {
      populateTimeFields();
    }
  }

  /**
   * Populate the fields from the includeFiledStr
   */
  private void populateIncludeFields()
  {
    includeFields = new String[2][];
    String[] portFields = includeFieldStr.split(";");
    for (int i = 0; i < portFields.length; i++) {
      includeFields[i] = portFields[i].split(",");
    }
  }

  /**
   * Get the tuples from another store based on join constraint and key
   *
   * @param tuple  input
   * @param isLeft whether the given tuple is from first port or not
   */
  private void join(TimeEvent tuple, Boolean isLeft)
  {
    // Get the valid tuples from the store based on key
    // If the tuple is null means the join type is outer and return unmatched tuples from store.
    Object value;
    if (isLeft) {
      if (tuple != null) {
        value = store[1].getValidTuples(tuple);
      } else {
        value = store[1].getUnMatchedTuples();
      }
    } else {
      if (tuple != null) {
        value = store[0].getValidTuples(tuple);
      } else {
        value = store[0].getUnMatchedTuples();
      }
    }
    // Join the input tuple with the joined tuples
    if (value != null) {
      ArrayList<TimeEvent> joinedValues = (ArrayList<TimeEvent>)value;
      List<T> result = new ArrayList<T>();
      for (int idx = 0; idx < joinedValues.size(); idx++) {
        T output = createOutputTuple();
        Object tupleValue = null;
        if (tuple != null) {
          tupleValue = tuple.getValue();
        }
        copyValue(output, tupleValue, isLeft);
        copyValue(output, (joinedValues.get(idx)).getValue(), !isLeft);
        result.add(output);
        (joinedValues.get(idx)).setMatch(true);
      }
      if (tuple != null) {
        tuple.setMatch(true);
      }
      if (result.size() != 0) {
        outputPort.emit(result);
      }
    }
  }

  // Emit the unmatched tuples, if the strategy is outer join
  @Override
  public void endWindow()
  {
    if (strategy.equals(JoinStrategy.LEFT_OUTER_JOIN) || strategy.equals(JoinStrategy.OUTER_JOIN)) {
      join(null, false);
    }
    if (strategy.equals(JoinStrategy.RIGHT_OUTER_JOIN) || strategy.equals(JoinStrategy.OUTER_JOIN)) {
      join(null, true);
    }
    store[0].endWindow();
    store[1].endWindow();
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

  /**
   * Create the event
   *
   * @param tuple
   * @return
   */
  protected TimeEvent createEvent(Object tuple)
  {
    int idx = 0;
    if (!isLeft) {
      idx = 1;
    }
    if (timeFields != null) {
      return new TimeEventImpl(getKeyValue(keys[idx], tuple), (Long)getTime(timeFields[idx], tuple), tuple);
    } else {
      return new TimeEventImpl(getKeyValue(keys[idx], tuple), Calendar.getInstance().getTimeInMillis(), tuple);
    }
  }

  public void populateKeyFields()
  {
    this.keys = keyFieldStr.split(",");
  }

  public String getIncludeFieldStr()
  {
    return includeFieldStr;
  }

  public JoinStrategy getStrategy()
  {
    return strategy;
  }

  public void setStrategy(JoinStrategy strategy)
  {
    this.strategy = strategy;
  }

  public BackupStore getLeftStore()
  {
    return store[0];
  }

  public void setLeftStore(BackupStore lStore)
  {
    store[0] = lStore;
  }

  public BackupStore getRightStore()
  {
    return store[1];
  }

  public void setRightStore(BackupStore rStore)
  {
    store[1] = rStore;
  }

  public String getKeyFields()
  {
    return keyFieldStr;
  }

  public void setKeyFields(String keyFieldStr)
  {
    this.keyFieldStr = keyFieldStr;
  }

  public String getTimeFields()
  {
    return timeFieldStr;
  }

  public void setTimeFields(String timeFieldStr)
  {
    this.timeFieldStr = timeFieldStr;
  }

  public String getIncludeFields()
  {
    return includeFieldStr;
  }

  public void setIncludeFields(String includeFieldStr)
  {
    this.includeFieldStr = includeFieldStr;
  }

  /**
   * Specify the comma separated time fields for both steams
   */
  public void populateTimeFields()
  {
    this.timeFields = timeFieldStr.split(",");
  }

  public void setStrategy(String policy)
  {
    this.strategy = JoinStrategy.valueOf(policy.toUpperCase());
  }

  /**
   * Create the output object
   *
   * @return
   */
  protected abstract T createOutputTuple();

  /**
   * Get the values from extractTuple and set these values to the output
   *
   * @param output
   * @param extractTuple
   * @param isLeft
   */
  protected abstract void copyValue(T output, Object extractTuple, Boolean isLeft);

  /**
   * Get the value of the key field from the given tuple
   *
   * @param keyField
   * @param tuple
   * @return
   */
  protected abstract Object getKeyValue(String keyField, Object tuple);

  /**
   * Get the value of the time field from the given tuple
   *
   * @param field
   * @param tuple
   * @return
   */
  protected abstract Object getTime(String field, Object tuple);

  public static enum JoinStrategy
  {
    INNER_JOIN,
    LEFT_OUTER_JOIN,
    RIGHT_OUTER_JOIN,
    OUTER_JOIN
  }
}
