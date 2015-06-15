package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.join.InMemoryStore;
import com.datatorrent.contrib.join.POJOJoinOperator;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="GenericSalesBeanJoinDemo")
public class BeanJoinApp implements StreamingApplication
{
  public static class CollectorModule extends BaseOperator
  {
    public final transient DefaultInputPort<Object> inputPort = new DefaultInputPort<Object>()
    {

      @Override
      public void process(Object arg0)
      {
      }
    };

  }

  @Override public void populateDAG(DAG dag, Configuration conf)
  {
    JsonSalesGenerator input = dag.addOperator("Input", JsonSalesGenerator.class);
    input.setAddProductCategory(false);
    input.setMaxTuplesPerWindow(200);
    input.setTuplesPerWindowDeviation(0);

    JsonProductGenerator input2 = dag.addOperator("Prodcut", JsonProductGenerator.class);
    input2.setMaxTuplesPerWindow(200);
    input2.setTuplesPerWindowDeviation(0);

    POJOJoinOperator joinOper = dag.addOperator("Join", new POJOJoinOperator());
    joinOper.setLeftStore(new InMemoryStore(60000 * 10 * 2, 60000 * 5));
    joinOper.setRightStore(new InMemoryStore(60000 * 10 * 2, 60000 * 5));
    joinOper.setIncludeFieldStr("timestamp,customerId,productId,regionId,amount;productCategory");
    joinOper.setKeyFields("productId,productId");

    joinOper.setOutputClass("com.datatorrent.demos.dimensions.generic.SalesEvent");
    CollectorModule console = dag.addOperator("Console", new CollectorModule());

    //dag.setInputPortAttribute(converter.input, Context.PortContext.PARTITION_PARALLEL, true);
    // Removing setLocality(Locality.CONTAINER_LOCAL) from JSONStream and MapStream to isolate performance bottleneck
    dag.addStream("SalesInput", input.outputPort, joinOper.input1);
    dag.addStream("JsonProductStream", input2.outputPort, joinOper.input2);
    dag.addStream("Output", joinOper.outputPort, console.inputPort).setLocality(DAG.Locality.CONTAINER_LOCAL);

  }
}