package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.join.MapJoinOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="GenericSalesMapJoinDemo")
public class MapJoinDemo implements StreamingApplication
{
  @Override public void populateDAG(DAG dag, Configuration conf)
  {
    JsonSalesGenerator input = dag.addOperator("Input", JsonSalesGenerator.class);
    input.setAddProductCategory(false);
    input.setMaxTuplesPerWindow(100);
    input.setTuplesPerWindowDeviation(0);

    JsonToMapConverter converter = dag.addOperator("Parse", JsonToMapConverter.class);

    JsonProductGenerator input2 = dag.addOperator("Prodcut", JsonProductGenerator.class);
    input2.setMaxTuplesPerWindow(20);
    input2.setTuplesPerWindowDeviation(0);

    MapJoinOperator joinOper = dag.addOperator("Join", new MapJoinOperator());
    joinOper.setExpiryTime(3000);
    joinOper.setBucketSpanInMillis(1000);
    joinOper.setIncludeFieldStr("timestamp,customerId,productId,regionId,amount;productCategory");
    joinOper.setKeyFields("productId,productId");

    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    //dag.setInputPortAttribute(converter.input, Context.PortContext.PARTITION_PARALLEL, true);
    // Removing setLocality(Locality.CONTAINER_LOCAL) from JSONStream and MapStream to isolate performance bottleneck
    dag.addStream("SalesInput", input.jsonBytes, converter.input).setLocality(DAG.Locality.THREAD_LOCAL);
    dag.addStream("JSONStream", converter.outputMap, joinOper.input1);
    dag.addStream("JsonProductStream", input2.outputMap, joinOper.input2);
    dag.addStream("Output", joinOper.outputPort, console.input);

  }
}
