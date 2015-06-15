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
package com.datatorrent.contrib.join;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import org.slf4j.LoggerFactory;

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
  // Stores for each of the input port
  private BackupStore store[] = (BackupStore[]) Array.newInstance(BackupStore.class, 2);
  // Fields to compare from both the streams

  protected String[] timeFields;

  protected String[][] includeFields;

  private String includeFieldStr;

  protected String[] keys;
  protected JoinStrategy strategy = JoinStrategy.INNER_JOIN;

  public final transient DefaultOutputPort<List<T>> outputPort = new DefaultOutputPort<List<T>>();

  @Override
  public void beginWindow(long windowId) {
    store[0].beginWindow(windowId);
    store[1].beginWindow(windowId);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    if(store[0] == null) {
      logger.error("Left Store is Empty");
    }
    if(store[1] == null) {
      logger.error("Right Store is Empty");
    }
    boolean isOuter = strategy.equals(JoinStrategy.LEFT_OUTER_JOIN) || strategy.equals(JoinStrategy.OUTER_JOIN);
    store[0].isOuterJoin(isOuter);
    isOuter = strategy.equals(JoinStrategy.RIGHT_OUTER_JOIN) || strategy.equals(JoinStrategy.OUTER_JOIN);
    store[1].isOuterJoin(isOuter);
    // Setup the stores
    store[0].setup();
    store[1].setup();

    populateIncludeFields();
  }

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<T> input1 = new DefaultInputPort<T>()
  {
    @Override public void process(T tuple)
    {
      processTuple(tuple, true);
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<T> input2 = new DefaultInputPort<T>()
  {
    @Override public void process(T tuple)
    {
      processTuple(tuple, false);
    }
  };

  protected void processTuple(T tuple, Boolean isLeft)
  {

    int idx = 0;
    if(!isLeft) {
      idx = 1;
    }
    TimeEvent t = createEvent(isLeft, tuple);
    store[idx].put(t);
    join(t, isLeft);
  }
  /**
   * Populate the fields from the includeFiledStr
   */
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
   * @param isLeft whether the given tuple is from first port or not
   */
  private void join(TimeEvent tuple, Boolean isLeft)
  {
    // Get the valid tuples from the store based on key
    // If the tuple is null means the join type is outer and return unmatched tuples from store.
    Object value ;
    if(isLeft) {
      if(tuple != null) {
        value = store[1].getValidTuples(tuple);
      } else {
        value = store[1].getUnMatchedTuples();
      }
    } else {
      if(tuple != null) {
        value = store[0].getValidTuples(tuple);
      } else {
        value = store[0].getUnMatchedTuples();
      }
    }
    // Join the input tuple with the joined tuples
    if(value != null) {
      ArrayList<TimeEvent> joinedValues = (ArrayList<TimeEvent>)value;
      List<T> result = new ArrayList<T>();
      for(int idx = 0; idx < joinedValues.size(); idx++) {
        T output = createOutputTuple();
        Object tupleValue = null;
        if(tuple != null) {
          tupleValue = tuple.getValue();
        }
        copyValue(output, tupleValue, isLeft);
        copyValue(output, (joinedValues.get(idx)).getValue(), !isLeft);
        result.add(output);
        (joinedValues.get(idx)).setMatch(true);
      }
      if(tuple != null) {
        tuple.setMatch(true);
      }
      if(result.size() != 0) {
        outputPort.emit(result);
      }
    }
  }

  // Emit the unmatched tuples, if the strategy is outer join
  @Override
  public void endWindow()
  {
    if(strategy.equals(JoinStrategy.LEFT_OUTER_JOIN) || strategy.equals(JoinStrategy.OUTER_JOIN)) {
      join(null, false);
    }
    if(strategy.equals(JoinStrategy.RIGHT_OUTER_JOIN) || strategy.equals(JoinStrategy.OUTER_JOIN)) {
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
   * @param isLeft
   * @param tuple
   * @return
   */
  protected TimeEvent createEvent(Boolean isLeft, Object tuple)
  {
    int idx = 0;
    if(!isLeft) {
      idx = 1;
    }
    if(timeFields != null)
      return new TimeEvent(getKeyValue(keys[idx], tuple), (Long) getTime(timeFields[idx], tuple), tuple);
    else
      return new TimeEvent(getKeyValue(keys[idx], tuple), Calendar.getInstance().getTimeInMillis(), tuple);
  }

  public void setKeyFields(@NonNull String keyFields)
  {
    this.keys = keyFields.split(",");
  }

  public void setIncludeFieldStr(String includeFieldStr)
  {
    this.includeFieldStr = includeFieldStr;
  }

  public void setLeftStore(BackupStore lStore)
  {
    store[0] = lStore;
  }

  public void setRightStore(BackupStore rStore)
  {
    store[1] = rStore;
  }

  /**
   * Specify the comma separated time fields for both steams
   * @param timeFields
   */
  public void setTimeFields(String timeFields)
  {
    this.timeFields = timeFields.split(",");
  }

  public static enum JoinStrategy
  {
    INNER_JOIN,
    LEFT_OUTER_JOIN,
    RIGHT_OUTER_JOIN,
    OUTER_JOIN
  }

  public void setStrategy(String policy)
  {
    this.strategy = JoinStrategy.valueOf(policy.toUpperCase());
  }

  public void setStrategy(JoinStrategy strategy)
  {
    this.strategy = strategy;
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
   * @param isLeft
   */
  protected abstract void copyValue(T output, Object extractTuple, Boolean isLeft);

  /**
   * Get the value of the key field from the given tuple
   * @param keyField
   * @param tuple
   * @return
   */
  protected abstract Object getKeyValue(String keyField, Object tuple);

  /**
   * Get the value of the time field from the given tuple
   * @param field
   * @param tuple
   * @return
   */
  protected abstract Object getTime(String field, Object tuple);
}
