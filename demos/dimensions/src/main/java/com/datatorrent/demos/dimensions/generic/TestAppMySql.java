package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.enrichment.JDBCLoader;
import com.datatorrent.contrib.enrichment.MapEnrichmentOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="TestAppMysql")
public class TestAppMySql implements StreamingApplication
{

  @Override public void populateDAG(DAG dag, Configuration conf)
  {
    JsonSalesGenerator input = dag.addOperator("Input", JsonSalesGenerator.class);
    input.setAddProductCategory(false);
    input.setMaxTuplesPerWindow(100);
    JsonToMapConverter converter = dag.addOperator("Parse", JsonToMapConverter.class);

    MapEnrichmentOperator enrichmentOperator = dag.addOperator("Enrichment", new MapEnrichmentOperator());
    JDBCLoader store = new JDBCLoader();
    store.setDbDriver("org.gjt.mm.mysql.Driver");
    store.setDbUrl("jdbc:mysql://localhost/enrichment");
    store.setUserName("root");
    store.setPassword("test");
    store.setTableName("productmapping");

    enrichmentOperator.setStore(store);
    enrichmentOperator.setLookupFieldsStr("productId");
    enrichmentOperator.setIncludeFieldsStr("productCategory");

    ConsoleOutputOperator out1 = dag.addOperator("Console1", new ConsoleOutputOperator());
    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    //dag.setInputPortAttribute(converter.input, Context.PortContext.PARTITION_PARALLEL, true);
    // Removing setLocality(Locality.CONTAINER_LOCAL) from JSONStream and MapStream to isolate performance bottleneck
    dag.addStream("JSONStream", input.jsonBytes, converter.input);
    dag.addStream("MapStream", converter.outputMap, out1.input, enrichmentOperator.input);
    dag.addStream("Output", enrichmentOperator.output, console.input);
  }
}
