package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.*;
import com.datatorrent.contrib.enrichment.*;
import com.datatorrent.lib.io.*;
import org.apache.hadoop.conf.*;

@ApplicationAnnotation(name="GenericSalesMapEnrichmentWithFSStore")
public class GenericSalesMapEnrichmentWithFSStore implements StreamingApplication
{

  @Override public void populateDAG(DAG dag, Configuration conf)
  {
    JsonSalesGenerator input = dag.addOperator("Input", JsonSalesGenerator.class);
    input.setAddProductCategory(false);
    input.setMaxTuplesPerWindow(100);
    JsonToMapConverter converter = dag.addOperator("Parse", JsonToMapConverter.class);

    MapEnrichmentOperator enrichmentOperator = dag.addOperator("Enrichment", new MapEnrichmentOperator());
    FSLoader fsstore = new FSLoader();
    fsstore.setFileName(conf.get("dt.application.GenericSalesMapEnrichmentWithFSStore.operator.store.fileName"));
    enrichmentOperator.setStore(fsstore);

    ConsoleOutputOperator out1 = dag.addOperator("Console1", new ConsoleOutputOperator());
    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    //dag.setInputPortAttribute(converter.input, Context.PortContext.PARTITION_PARALLEL, true);
    // Removing setLocality(Locality.CONTAINER_LOCAL) from JSONStream and MapStream to isolate performance bottleneck
    dag.addStream("JSONStream", input.jsonBytes, converter.input);
    dag.addStream("MapStream", converter.outputMap, out1.input, enrichmentOperator.input);
    dag.addStream("Output", enrichmentOperator.output, console.input);
  }
}