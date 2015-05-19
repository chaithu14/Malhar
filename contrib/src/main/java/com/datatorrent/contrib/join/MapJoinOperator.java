package com.datatorrent.contrib.join;

import java.util.HashMap;
import java.util.Map;

public class MapJoinOperator extends AbstractJoinOperator<Map<String, Object>>
{
  @Override protected Map<String, Object> createOutputTuple()
  {
    return new HashMap<String, Object>();
  }

  @Override protected void addValue(Map<String, Object> output, Object extractTuple, Boolean isFirst)
  {
    String[] fields ;
    if(isFirst)
      fields = includeFields[0];
    else
      fields = includeFields[1];
    for(int i=0; i < fields.length; i++) {
      output.put(fields[i], ((Map<String, Object>)extractTuple).get(fields[i]));
    }
  }

  public Object getValue(String keyField, Object tuple)
  {
    Map<String, Object> o = (Map<String, Object>)tuple;
    return o.get(keyField);
  }
}
