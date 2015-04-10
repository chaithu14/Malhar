package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.enrichment.BeanEnrichmentOperator;
import com.datatorrent.contrib.enrichment.HBaseLoader;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;


@ApplicationAnnotation(name="TestBeanAppHBase")
public class TestBeanAppHBase implements StreamingApplication
{

  @Override public void populateDAG(DAG dag, Configuration conf)
  {
    JsonSalesGenerator input = dag.addOperator("Input", JsonSalesGenerator.class);
    input.setAddProductCategory(false);
    input.setMaxTuplesPerWindow(100);
    JsonToSalesEventConverter converter = dag.addOperator("Parse", new JsonToSalesEventConverter());

    BeanEnrichmentOperator enrichmentOperator = dag.addOperator("Enrichment", new BeanEnrichmentOperator());
    HBaseLoader store = new HBaseLoader();
    store.setZookeeperQuorum("localhost");
    store.setZookeeperClientPort(2181);
    store.setTableName("productmapping");
    store.setIncludeFamilyStr("product");

    enrichmentOperator.inputClass = SalesData.class;
    enrichmentOperator.outputClass = SalesData.class;
    enrichmentOperator.setStore(store);
    enrichmentOperator.setIncludeFieldsStr("productCategory");
    enrichmentOperator.setLookupFieldsStr("productId");

    ConsoleOutputOperator out1 = dag.addOperator("Console1", new ConsoleOutputOperator());
    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    // Removing setLocality(Locality.CONTAINER_LOCAL) from JSONStream and MapStream to isolate performance bottleneck
    dag.addStream("JSONStream", input.jsonBytes, converter.input);
    dag.addStream("MapStream", converter.outputMap, out1.input, enrichmentOperator.input);
    dag.addStream("Output", enrichmentOperator.output, console.input);
  }
}

