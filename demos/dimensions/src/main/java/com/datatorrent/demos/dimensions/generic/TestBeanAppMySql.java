package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.contrib.enrichment.BeanEnrichmentOperator;
import com.datatorrent.contrib.enrichment.JDBCLoader;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Stateless
class JsonToSalesEventConverter extends BaseOperator
{

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final ObjectReader reader = mapper.reader(new TypeReference<SalesData>() { });
  private static final Logger logger = LoggerFactory.getLogger(JsonToMapConverter.class);

  /**
   * Accepts JSON formatted byte arrays
   */
  public final transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] message)
    {
      try {
        // Convert byte array JSON representation to HashMap
        SalesData tuple = reader.readValue(message);
        outputMap.emit(tuple);
      }
      catch (Throwable ex) {
        DTThrowable.rethrow(ex);
      }
    }
  };

  /**
   * Output JSON converted to Map<string,Object>
   */
  public final transient DefaultOutputPort<SalesData> outputMap = new DefaultOutputPort<SalesData>();

}

@ApplicationAnnotation(name="TestBeanAppMysql")
public class TestBeanAppMySql implements StreamingApplication
{

  @Override public void populateDAG(DAG dag, Configuration conf)
  {
    JsonSalesGenerator input = dag.addOperator("Input", JsonSalesGenerator.class);
    input.setAddProductCategory(false);
    input.setMaxTuplesPerWindow(100);
    JsonToSalesEventConverter converter = dag.addOperator("Parse", new JsonToSalesEventConverter());

    //MapEnrichmentOperator enrichmentOperator = dag.addOperator("Enrichment", new MapEnrichmentOperator());
    BeanEnrichmentOperator enrichmentOperator = dag.addOperator("Enrichment", new BeanEnrichmentOperator());
    JDBCLoader store = new JDBCLoader();
    store.setDbDriver("org.gjt.mm.mysql.Driver");
    store.setDbUrl("jdbc:mysql://localhost/enrichment");
    store.setUserName("root");
    store.setPassword("test");
    store.setTableName("productmapping");

    //Map<String, Object> ob = new HashMap<String, Object>();
    enrichmentOperator.inputClass = SalesData.class;
    enrichmentOperator.outputClass = SalesData.class;
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
