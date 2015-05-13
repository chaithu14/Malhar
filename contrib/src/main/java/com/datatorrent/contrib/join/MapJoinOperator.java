package com.datatorrent.contrib.join;

import java.util.HashMap;
import java.util.Map;

public class MapJoinOperator extends AbstractJoinOperator<Map<String, Object>>
{
  @Override protected Map<String, Object> createOutputTuple()
  {
    return new HashMap<String, Object>();
  }

  @Override protected void addValue(Map<String, Object> output, String field, Object extractTuple)
  {
    output.put(field, ((Map<String, Object>)extractTuple).get(field));
  }

  public Object getValue(String keyField, Object tuple)
  {
    Map<String, Object> o = (Map<String, Object>)tuple;
    return o.get(keyField);
  }
}
