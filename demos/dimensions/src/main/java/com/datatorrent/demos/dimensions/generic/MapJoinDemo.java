package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.DAG;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.join.MapJoinOperator;
import com.datatorrent.contrib.join.HDHTBasedStore;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="GenericSalesMapJoinDemo")
public class MapJoinDemo implements StreamingApplication
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
    input.setMaxTuplesPerWindow(500);
    input.setTuplesPerWindowDeviation(0);

    JsonToMapConverter converter = dag.addOperator("Parse", JsonToMapConverter.class);

    JsonProductGenerator input2 = dag.addOperator("Prodcut", JsonProductGenerator.class);
    input2.setMaxTuplesPerWindow(200);
    input2.setTuplesPerWindowDeviation(0);

    MapJoinOperator joinOper = dag.addOperator("Join", new MapJoinOperator());
    joinOper.setLeftStore(new HDHTBasedStore(3000, "buckets/UP/", 1));
    joinOper.setRightStore(new HDHTBasedStore(3000, "buckets/DOWN/", 1));
    
    joinOper.setIncludeFieldStr("timestamp,customerId,productId,regionId,amount;productCategory");
    joinOper.setKeyFields("productId,productId");

    CollectorModule console = dag.addOperator("Console", new CollectorModule());

    //dag.setInputPortAttribute(converter.input, Context.PortContext.PARTITION_PARALLEL, true);
    // Removing setLocality(Locality.CONTAINER_LOCAL) from JSONStream and MapStream to isolate performance bottleneck
    dag.addStream("SalesInput", input.jsonBytes, converter.input).setLocality(DAG.Locality.THREAD_LOCAL);
    dag.addStream("JSONStream", converter.outputMap, joinOper.input1);
    dag.addStream("JsonProductStream", input2.outputMap, joinOper.input2);
    dag.addStream("Output", joinOper.outputPort, console.inputPort).setLocality(DAG.Locality.CONTAINER_LOCAL);

  }
}
