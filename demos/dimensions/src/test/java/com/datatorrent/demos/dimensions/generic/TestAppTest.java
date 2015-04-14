package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.*;
import org.apache.hadoop.conf.*;
import org.junit.*;

public class TestAppTest
{
  @Test
  public void test() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    lma.prepareDAG(new GenericSalesBeanEnrichmentWithJDBCStore(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.run(10000);
  }

}
