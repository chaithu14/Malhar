package com.datatorrent.contrib.join;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
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
    oper.setExpiryTime(200);
    oper.setIncludeFieldStr("ID,Name;OID,Amount");
    oper.setKeyFields("ID,CID");

    oper.setup(null);

    CollectorTestSink<List<Map<String, Object>>> sink = new CollectorTestSink<List<Map<String, Object>>>();
    @SuppressWarnings({ "unchecked", "rawtypes" }) CollectorTestSink<Object> tmp = (CollectorTestSink) sink;
    oper.outputPort.setSink(tmp);

    oper.beginWindow(0);
    Map<String, Object> tuple = Maps.newHashMap();
    tuple.put("ID", 1);
    tuple.put("Name","Anil");

    Kryo kryo = new Kryo();
    oper.input1.process(kryo.copy(tuple));

    CountDownLatch latch = new CountDownLatch(1);
    //latch.await(200, TimeUnit.MILLISECONDS);
    Map<String, Object> order = Maps.newHashMap();
    order.put("OID", 102);
    order.put("CID",1);
    order.put("Amount",300);

    oper.input2.process(kryo.copy(order));

   Map<String, Object> order1 = Maps.newHashMap();
    order1.put("OID", 103);
    order1.put("CID",3);
    order1.put("Amount",300);

    oper.input2.process(kryo.copy(order1));
    latch.await(200, TimeUnit.MILLISECONDS);

    Map<String, Object> order2 = Maps.newHashMap();
    order2.put("OID", 104);
    order2.put("CID",1);
    order2.put("Amount",300);

    oper.input2.process(kryo.copy(order2));

    latch.await(100, TimeUnit.MILLISECONDS);

    oper.endWindow();

    /* Number of tuple, emitted */
    Assert.assertEquals("Number of tuple emitted ", 1, sink.collectedTuples.size());
    List<Map<String, Object>> emittedList = sink.collectedTuples.iterator().next();
    Map<String, Object> emitted = emittedList.get(0);

    /* The fields present in original event is kept as it is */
    Assert.assertEquals("Number of fields in emitted tuple", 4, emitted.size());
    Assert.assertEquals("value of ID :", tuple.get("ID"), emitted.get("ID"));
    Assert.assertEquals("value of Name :", tuple.get("Name"), emitted.get("Name"));

    Assert.assertEquals("value of OID: ", order.get("OID"), emitted.get("OID"));
    Assert.assertEquals("value of Amount: ", order.get("Amount"), emitted.get("Amount"));

  }
}
