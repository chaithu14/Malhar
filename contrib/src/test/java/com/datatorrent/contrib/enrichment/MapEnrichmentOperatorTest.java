package com.datatorrent.contrib.enrichment;

import com.beust.jcommander.internal.Maps;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MapEnrichmentOperatorTest
{
  @Test
  public void includeAllKeys()
  {
    MapEnrichmentOperator oper = new MapEnrichmentOperator();
    oper.setStore(new MemoryStore());
    oper.setLookupFieldsStr("In1");
    oper.setup(null);

    CollectorTestSink sink = new CollectorTestSink();
    TestUtils.setSink(oper.output, sink);

    Map<String, Object> inMap = Maps.newHashMap();
    inMap.put("In1", "Value1");
    inMap.put("In2", "Value2");

    oper.beginWindow(1);
    oper.input.process(inMap);
    oper.endWindow();

    System.out.println("Number of tuples emitted " + sink.collectedTuples.size());
    System.out.println(sink.collectedTuples.get(0));

  }

  @Test
  public void includeSelectedKeys()
  {
    MapEnrichmentOperator oper = new MapEnrichmentOperator();
    oper.setStore(new MemoryStore());
    oper.setLookupFieldsStr("In1");
    oper.setIncludeFieldsStr("A,B");
    oper.setup(null);

    CollectorTestSink sink = new CollectorTestSink();
    TestUtils.setSink(oper.output, sink);

    Map<String, Object> inMap = Maps.newHashMap();
    inMap.put("In1", "Value1");
    inMap.put("In2", "Value2");

    oper.beginWindow(1);
    oper.input.process(inMap);
    oper.endWindow();

    System.out.println("Number of tuples emitted " + sink.collectedTuples.size());
    System.out.println(sink.collectedTuples.get(0));
  }

  private static class MemoryStore implements EnrichmentBackup
  {
    static Map<String, Map> returnData = Maps.newHashMap();

    static {
      Map<String, String> map = Maps.newHashMap();
      map.put("A", "Val_A");
      map.put("B", "Val_B");
      map.put("C", "Val_C");
      map.put("In1", "Value3");
      returnData.put("Value1", map);

      map = Maps.newHashMap();
      map.put("A", "Val_A_1");
      map.put("B", "Val_B_1");
      map.put("C", "Val_C");
      map.put("In1", "Value3");
      returnData.put("Value2", map);
    }

    @Override public Map<Object, Object> loadInitialData()
    {
      return null;
    }

    @Override public Object get(Object key)
    {
      List<String> keyList = (List<String>)key;
      return returnData.get(keyList.get(0));
    }

    @Override public List<Object> getAll(List<Object> keys)
    {
      return null;
    }

    @Override public void put(Object key, Object value)
    {

    }

    @Override public void putAll(Map<Object, Object> m)
    {

    }

    @Override public void remove(Object key)
    {

    }

    @Override public void connect() throws IOException
    {

    }

    @Override public void disconnect() throws IOException
    {

    }

    @Override public boolean isConnected()
    {
      return false;
    }

    @Override public void setFields(List<String> lookupFields, List<String> includeFields)
    {

    }

    @Override
    public boolean needRefresh() {
      return false;
    }
  }
}
