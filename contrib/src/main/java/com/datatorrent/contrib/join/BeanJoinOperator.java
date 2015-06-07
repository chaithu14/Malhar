package com.datatorrent.contrib.join;

import com.datatorrent.lib.util.PojoUtils;
import org.apache.commons.lang3.ClassUtils;

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
    /*try {
      return tuple.getClass().getField(keyField).get(tuple);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    }
    return null;*/
    PojoUtils.Getter getter = null;
    try {
      getter = PojoUtils.createGetter(tuple.getClass(), keyField, ClassUtils.primitiveToWrapper(tuple.getClass().getField(keyField).getType()));
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    }
    return getter.get(tuple);
  }
}
