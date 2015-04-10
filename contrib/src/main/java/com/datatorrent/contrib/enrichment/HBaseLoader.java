package com.datatorrent.contrib.enrichment;

import com.datatorrent.contrib.hbase.HBaseStore;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseLoader extends HBaseStore implements EnrichmentBackup
{
  protected transient List<String> includeFields;
  protected transient List<String> lookupFields;
  protected transient List<String> includeFamilys;

  protected Object getQueryResult(Object key)
  {
    try {
      Get get = new Get(getRowBytes(key));
      int idx = 0;
      for(String f : includeFields) {
        get.addColumn(Bytes.toBytes(includeFamilys.get(idx++)), Bytes.toBytes(f));
      }
      return getTable().get(get);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected ArrayList<Object> getDataFrmResult(Object result)
  {
    Result res = (Result)result;
    if (res == null || res.isEmpty())
      return null;
    ArrayList<Object> columnInfo = new ArrayList<Object>();

    for(KeyValue kv : res.raw()) {
      columnInfo.add(kv.getValue());
    }
    return columnInfo;
  }

  private byte[] getRowBytes(Object key)
  {
    return ((String)key).getBytes();
  }

  @Override public void setLookupFields(List<String> lookupFields)
  {
    this.lookupFields = lookupFields;
  }

  @Override public void setIncludeFields(List<String> includeFields)
  {
    this.includeFields = includeFields;
  }

  public void setIncludeFamilyStr(String familyStr)
  {
    this.includeFamilys = Arrays.asList(familyStr.split(","));
  }

  @Override public boolean needRefresh()
  {
    return false;
  }

  @Override public Map<Object, Object> loadInitialData()
  {
    return null;
  }

  @Override public Object get(Object key)
  {
    return getDataFrmResult(getQueryResult(key));
  }

  @Override public List<Object> getAll(List<Object> keys)
  {
    List<Object> values = Lists.newArrayList();
    for (Object key : keys) {
      values.add(get(key));
    }
    return values;
  }

  @Override public void put(Object key, Object value)
  {
    throw new RuntimeException("Not supported operation");
  }

  @Override public void putAll(Map<Object, Object> m)
  {
    throw new RuntimeException("Not supported operation");
  }

  @Override public void remove(Object key)
  {
    throw new RuntimeException("Not supported operation");
  }
}
