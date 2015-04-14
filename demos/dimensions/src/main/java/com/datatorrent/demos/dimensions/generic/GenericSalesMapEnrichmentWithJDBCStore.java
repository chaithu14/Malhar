package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.enrichment.JDBCLoader;
import com.datatorrent.contrib.enrichment.MapEnrichmentOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="GenericSalesMapEnrichmentWithJDBCStore")
public class GenericSalesMapEnrichmentWithJDBCStore implements StreamingApplication
{

  @Override public void populateDAG(DAG dag, Configuration conf)
  {
    JsonSalesGenerator input = dag.addOperator("Input", JsonSalesGenerator.class);
    input.setAddProductCategory(false);
    input.setMaxTuplesPerWindow(100);
    JsonToMapConverter converter = dag.addOperator("Parse", JsonToMapConverter.class);

    MapEnrichmentOperator enrichmentOperator = dag.addOperator("Enrichment", new MapEnrichmentOperator());
    JDBCLoader store = new JDBCLoader();
    store.setDbDriver(conf.get("dt.application.GenericSalesMapEnrichmentWithJDBCStore.operator.store.dbDriver"));
    store.setDbUrl(conf.get("dt.application.GenericSalesMapEnrichmentWithJDBCStore.operator.store.dbUrl"));
    store.setUserName(conf.get("dt.application.GenericSalesMapEnrichmentWithJDBCStore.operator.store.userName"));
    store.setPassword(conf.get("dt.application.GenericSalesMapEnrichmentWithJDBCStore.operator.store.password"));
    store.setTableName(conf.get("dt.application.GenericSalesMapEnrichmentWithJDBCStore.operator.store.tableName"));

    enrichmentOperator.setStore(store);

    ConsoleOutputOperator out1 = dag.addOperator("Console1", new ConsoleOutputOperator());
    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    //dag.setInputPortAttribute(converter.input, Context.PortContext.PARTITION_PARALLEL, true);
    // Removing setLocality(Locality.CONTAINER_LOCAL) from JSONStream and MapStream to isolate performance bottleneck
    dag.addStream("JSONStream", input.jsonBytes, converter.input);
    dag.addStream("MapStream", converter.outputMap, out1.input, enrichmentOperator.input);
    dag.addStream("Output", enrichmentOperator.output, console.input);
  }
}
