package org.apache.apex.examples.parser.managedState;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "RocksDBApp")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    SequentialGenerator input = dag.addOperator("Input", new SequentialGenerator());
    SpillableMapRocksDBOperator access = dag.addOperator("Process", new SpillableMapRocksDBOperator());
    ConsoleOutputOperator output = dag.addOperator("Insert", new ConsoleOutputOperator());

    dag.addStream("inputToAccess", input.outputPort, access.inputPort);
    dag.addStream("OutputFromProcess", access.outputPort, output.input);

  }
}
