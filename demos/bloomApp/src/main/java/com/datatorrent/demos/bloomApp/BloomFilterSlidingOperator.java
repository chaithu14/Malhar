package com.datatorrent.demos.bloomApp;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.multiwindow.AbstractSlidingWindow;

public class BloomFilterSlidingOperator<T> extends AbstractSlidingWindow<T, BloomFilterOperatorObject<T>>
{
  private int expectedNumberOfElements;
  private double falsePositiveProbability;

  @Override
  protected void processDataTuple(T tuple)
  {
    states.get(currentCursor).add(tuple);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  @Override public BloomFilterOperatorObject<T> createWindowState()
  {
    BloomFilterOperatorObject<T> obj = states.get(currentCursor);
    if(obj == null)
      return new BloomFilterOperatorObject<T>(expectedNumberOfElements, falsePositiveProbability);
    else
    {
      obj.clear();
      return obj;
    }
  }

  @OutputPortFieldAnnotation(name = "CountOutput")
  public transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();


  @InputPortFieldAnnotation(name = "find")
  public final transient DefaultInputPort<T> find = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      for(int i = 0; i < getWindowSize(); i++)
      {
        BloomFilterOperatorObject<T> obj = states.get(i);
        if(obj != null && obj.contains(tuple)) {
          outputPort.emit(tuple);
          return ;
        }
      }
    }
  };

  public int getExpectedNumberOfElements()
  {
    return expectedNumberOfElements;
  }

  public void setExpectedNumberOfElements(int expectedNumberOfElements)
  {
    this.expectedNumberOfElements = expectedNumberOfElements;
  }

  public double getFalsePositiveProbability()
  {
    return falsePositiveProbability;
  }

  public void setFalsePositiveProbability(double falsePositiveProbability)
  {
    this.falsePositiveProbability = falsePositiveProbability;
  }
}
