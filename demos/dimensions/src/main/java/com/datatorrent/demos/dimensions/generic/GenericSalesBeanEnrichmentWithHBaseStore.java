package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.enrichment.BeanEnrichmentOperator;
import com.datatorrent.contrib.enrichment.HBaseLoader;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;


@ApplicationAnnotation(name="GenericSalesBeanEnrichmentWithHBaseStore")
public class GenericSalesBeanEnrichmentWithHBaseStore implements StreamingApplication
{

  @Override public void populateDAG(DAG dag, Configuration conf)
  {
    JsonSalesGenerator input = dag.addOperator("Input", JsonSalesGenerator.class);
    input.setAddProductCategory(false);
    input.setMaxTuplesPerWindow(100);
    JsonToSalesEventConverter converter = dag.addOperator("Parse", new JsonToSalesEventConverter());

    BeanEnrichmentOperator enrichmentOperator = dag.addOperator("Enrichment", new BeanEnrichmentOperator());
    HBaseLoader store = new HBaseLoader();
    store.setZookeeperQuorum(conf.get("dt.application.GenericSalesBeanEnrichmentWithHBaseStore.operator.store.zookeeperQuorum"));
    store.setZookeeperClientPort(Integer.parseInt(conf.get("dt.application.GenericSalesBeanEnrichmentWithHBaseStore.operator.store.zookeeperClientPort")));
    store.setTableName(conf.get("dt.application.GenericSalesBeanEnrichmentWithHBaseStore.operator.store.tableName"));
    store.setIncludeFamilyStr(conf.get("dt.application.GenericSalesBeanEnrichmentWithHBaseStore.operator.store.includeFamilyStr"));

    enrichmentOperator.setStore(store);

    ConsoleOutputOperator out1 = dag.addOperator("Console1", new ConsoleOutputOperator());
    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    // Removing setLocality(Locality.CONTAINER_LOCAL) from JSONStream and MapStream to isolate performance bottleneck
    dag.addStream("JSONStream", input.jsonBytes, converter.input);
    dag.addStream("MapStream", converter.outputMap, out1.input, enrichmentOperator.input);
    dag.addStream("Output", enrichmentOperator.output, console.input);
  }
}

