package com.datatorrent.contrib.join;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class MapTimeBasedJoinOperator
{
  @Rule public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  @Test public void testJoinOperator() throws IOException, InterruptedException
  {
    AbstractJoinOperator oper = new MapJoinOperator();
    oper.setLeftStore(new HDHTBasedStore(200, "buckets/UP/", 1));
    oper.setRightStore(new HDHTBasedStore(200, "buckets/DOWN/", 2));
    oper.setIncludeFieldStr("ID,Name;OID,Amount");
    oper.setKeyFields("ID,CID");

    oper.setup(null);

    CollectorTestSink<List<Map<String, Object>>> sink = new CollectorTestSink<List<Map<String, Object>>>();
    @SuppressWarnings({ "unchecked", "rawtypes" }) CollectorTestSink<Object> tmp = (CollectorTestSink) sink;
    oper.outputPort.setSink(tmp);

    oper.beginWindow(0);
    Map<String, Object> tuple1 = Maps.newHashMap();
    tuple1.put("ID", 1);
    tuple1.put("Name","Anil");

    oper.input1.process(tuple1);

    CountDownLatch latch = new CountDownLatch(1);
    Map<String, Object> order1 = Maps.newHashMap();
    order1.put("OID", 102);
    order1.put("CID",1);
    order1.put("Amount",300);

    oper.input2.process(order1);

    Map<String, Object> order2 = Maps.newHashMap();
    order2.put("OID", 103);
    order2.put("CID",3);
    order2.put("Amount",300);

    oper.input2.process(order2);
    latch.await(200, TimeUnit.MILLISECONDS);
    oper.endWindow();

    oper.beginWindow(1);
    Map<String, Object> tuple2 = Maps.newHashMap();
    tuple2.put("ID", 4);
    tuple2.put("Name","DT");
    oper.input1.process(tuple2);

    Map<String, Object> order3 = Maps.newHashMap();
    order3.put("OID", 104);
    order3.put("CID",1);
    order3.put("Amount",300);

    oper.input2.process(order2);

    latch.await(200, TimeUnit.MILLISECONDS);

    oper.endWindow();

    /* Number of tuple, emitted */
    Assert.assertEquals("Number of tuple emitted ", 1, sink.collectedTuples.size());
    List<Map<String, Object>> emittedList = sink.collectedTuples.iterator().next();
    Assert.assertEquals("Size of Joined Tuple ", 1, emittedList.size());
    Map<String, Object> emitted = emittedList.get(0);

    /* The fields present in original event is kept as it is */
    Assert.assertEquals("Number of fields in emitted tuple", 4, emitted.size());
    Assert.assertEquals("value of ID :", tuple1.get("ID"), emitted.get("ID"));
    Assert.assertEquals("value of Name :", tuple1.get("Name"), emitted.get("Name"));

    Assert.assertEquals("value of OID: ", order1.get("OID"), emitted.get("OID"));
    Assert.assertEquals("value of Amount: ", order1.get("Amount"), emitted.get("Amount"));


  }
}
