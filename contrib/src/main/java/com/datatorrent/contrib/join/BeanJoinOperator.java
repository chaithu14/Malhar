package com.datatorrent.contrib.join;

import com.datatorrent.lib.util.PojoUtils;
import java.util.ArrayList;
import java.util.List;

public class BeanJoinOperator extends AbstractJoinOperator
{
  public Class outputClass;
  private List<Class> inputClasses = new ArrayList<Class>();
  private List<PojoUtils.GetterObject> getters = new ArrayList<PojoUtils.GetterObject>();
  private final int noOfInputs = 2;

  @Override protected Object createOutputTuple()
  {
    try {
      return outputClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override protected void addValue(Object output, Object extractTuple, Boolean isFirst)
  {
    String[] fields ;
    if(isFirst)
      fields = includeFields[0];
    else
      fields = includeFields[1];
    for(int i=0; i < fields.length; i++) {
      try {
        outputClass.getField(fields[i]).set(output, getValue(fields[i], extractTuple));
      } catch (IllegalAccessException e) {
        throw  new RuntimeException(e);
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }

  }

  public Object getValue(String keyField, Object tuple)
  {
    if (inputClasses.size() != noOfInputs &&
        (inputClasses.size() == 0 || ((inputClasses.get(0) != tuple.getClass()) && inputClasses.size() == noOfInputs - 1))) {
      PojoUtils.GetterObject getter = PojoUtils.createGetterObject(tuple.getClass(), keyField);
      getters.add(getter);
      return getter.get(tuple);
    } else if (inputClasses.get(0) == tuple.getClass()) {
      return getters.get(0).get(tuple);
    }
    return getters.get(1).get(tuple);
  }
}
