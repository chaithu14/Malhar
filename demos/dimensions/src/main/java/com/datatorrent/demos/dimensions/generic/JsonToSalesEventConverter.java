package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.DTThrowable;
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
