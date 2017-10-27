package org.apache.apex.examples.parser.managedState;

import org.apache.commons.lang3.tuple.MutablePair;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

public class SequentialGenerator implements InputOperator
{
  private int maxTuplesPerWindow = 1;
  private long maxIntValue = Long.MAX_VALUE;
  private long nextTuple = 1;
  private int currentCount = 0;
  public transient DefaultOutputPort<MutablePair<Long, Long>> outputPort = new DefaultOutputPort<>();

  @Override
  public void beginWindow(long l)
  {
    currentCount = 0;
  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(Context.OperatorContext context)
  {

  }

  @Override
  public void teardown()
  {

  }

  @Override
  public void emitTuples()
  {
    while (currentCount < maxTuplesPerWindow) {
      outputPort.emit(new MutablePair<>(nextTuple, nextTuple));
      nextTuple = (nextTuple + 1) % maxIntValue;
      currentCount++;
    }
  }

  public int getMaxTuplesPerWindow()
  {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(int maxTuplesPerWindow)
  {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  public long getMaxIntValue()
  {
    return maxIntValue;
  }

  public void setMaxIntValue(long maxIntValue)
  {
    this.maxIntValue = maxIntValue;
  }
}
