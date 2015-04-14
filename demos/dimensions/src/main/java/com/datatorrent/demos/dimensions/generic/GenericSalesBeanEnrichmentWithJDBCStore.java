package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.enrichment.BeanEnrichmentOperator;
import com.datatorrent.contrib.enrichment.JDBCLoader;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;


@ApplicationAnnotation(name="GenericSalesBeanEnrichmentWithJDBCStore")
public class GenericSalesBeanEnrichmentWithJDBCStore implements StreamingApplication
{

  @Override public void populateDAG(DAG dag, Configuration conf)
  {
    JsonSalesGenerator input = dag.addOperator("Input", JsonSalesGenerator.class);
    input.setAddProductCategory(false);
    input.setMaxTuplesPerWindow(100);
    JsonToSalesEventConverter converter = dag.addOperator("Parse", new JsonToSalesEventConverter());

    BeanEnrichmentOperator enrichmentOperator = dag.addOperator("Enrichment", new BeanEnrichmentOperator());
    JDBCLoader store = new JDBCLoader();
    store.setDbDriver(conf.get("dt.application.GenericSalesBeanEnrichmentWithJDBCStore.operator.store.dbDriver"));
    store.setDbUrl(conf.get("dt.application.GenericSalesBeanEnrichmentWithJDBCStore.operator.store.dbUrl"));
    store.setUserName(conf.get("dt.application.GenericSalesBeanEnrichmentWithJDBCStore.operator.store.userName"));
    store.setPassword(conf.get("dt.application.GenericSalesBeanEnrichmentWithJDBCStore.operator.store.password"));
    store.setTableName(conf.get("dt.application.GenericSalesBeanEnrichmentWithJDBCStore.operator.store.tableName"));

    enrichmentOperator.inputClass = SalesData.class;
    enrichmentOperator.outputClass = SalesData.class;
    enrichmentOperator.setStore(store);

    ConsoleOutputOperator out1 = dag.addOperator("Console1", new ConsoleOutputOperator());
    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    // Removing setLocality(Locality.CONTAINER_LOCAL) from JSONStream and MapStream to isolate performance bottleneck
    dag.addStream("JSONStream", input.jsonBytes, converter.input);
    dag.addStream("MapStream", converter.outputMap, out1.input, enrichmentOperator.input);
    dag.addStream("Output", enrichmentOperator.output, console.input);
  }
}
