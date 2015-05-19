package com.datatorrent.contrib.join;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class BeanTimeBasedJoinOperatorTest
{

  @Rule public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  public static class Customer {
    public int ID;
    public String Name;

    public Customer()
    {

    }
    public Customer(int ID, String name)
    {
      this.ID = ID;
      Name = name;
    }

    @Override public String toString()
    {
      return "Customer{" +
          "ID=" + ID +
          ", Name='" + Name + '\'' +
          '}';
    }
  }

  public static class Order {
    public int OID;
    public int CID;
    public int Amount;

    public Order()
    {}

    public Order(int OID, int CID, int amount)
    {
      this.OID = OID;
      this.CID = CID;
      Amount = amount;
    }

    @Override public String toString()
    {
      return "Order{" +
          "OID=" + OID +
          ", CID=" + CID +
          ", Amount=" + Amount +
          '}';
    }
  }

  public static class CustOrder
  {
    public int ID;
    public String Name;
    public int OID;
    public int Amount;
    public CustOrder()
    {}

    @Override public String toString()
    {
      return "CustOrder{" +
          "ID=" + ID +
          ", Name='" + Name + '\'' +
          ", OID=" + OID +
          ", Amount=" + Amount +
          '}';
    }
  }
  @Test public void testJoinOperator() throws IOException, InterruptedException
  {

    BeanJoinOperator oper = new BeanJoinOperator();
    oper.setExpiryTime(200);
    oper.setIncludeFieldStr("ID,Name;OID,Amount");
    oper.setKeyFields("ID,CID");
    oper.outputClass = CustOrder.class;

    oper.setup(null);
    oper.activate(null);

    CollectorTestSink<List<CustOrder>> sink = new CollectorTestSink<List<CustOrder>>();
    @SuppressWarnings({ "unchecked", "rawtypes" }) CollectorTestSink<Object> tmp = (CollectorTestSink) sink;
    oper.outputPort.setSink(tmp);

    oper.beginWindow(0);

    Customer tuple = new Customer(1, "Anil");

    Kryo kryo = new Kryo();
    oper.input1.process(kryo.copy(tuple));

    CountDownLatch latch = new CountDownLatch(1);

    Order order = new Order(102, 1, 300);

    oper.input2.process(kryo.copy(order));

    Order order2 = new Order(103, 3, 300);
    oper.input2.process(kryo.copy(order2));

    Order order3 = new Order(104, 7, 300);
    oper.input2.process(kryo.copy(order3));

    latch.await(3000, TimeUnit.MILLISECONDS);

    oper.endWindow();

    /* Number of tuple, emitted */
    Assert.assertEquals("Number of tuple emitted ", 1, sink.collectedTuples.size());
    List<CustOrder> emittedList = sink.collectedTuples.iterator().next();
    CustOrder emitted = emittedList.get(0);

    Assert.assertEquals("value of ID :", tuple.ID, emitted.ID);
    Assert.assertEquals("value of Name :", tuple.Name, emitted.Name);


    Assert.assertEquals("value of OID: ", order.OID, emitted.OID);
    Assert.assertEquals("value of Amount: ", order.Amount, emitted.Amount);
    oper.deactivate();

  }
}
