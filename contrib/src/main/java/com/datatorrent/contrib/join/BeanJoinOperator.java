package com.datatorrent.contrib.join;

import com.datatorrent.lib.util.PojoUtils;

public class BeanJoinOperator extends AbstractJoinOperator
{
  public Class outputClass;

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
    PojoUtils.Getter getter = PojoUtils.createGetter(tuple.getClass(), keyField, Object.class);
    return getter.get(tuple);
  }
}
