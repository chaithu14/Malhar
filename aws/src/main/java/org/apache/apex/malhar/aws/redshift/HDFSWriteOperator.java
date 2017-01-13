package org.apache.apex.malhar.aws.redshift;

import java.io.IOException;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;

public class HDFSWriteOperator<INPUT> extends GenericFileOutputOperator<INPUT>
{
  protected static final String recoveryPath = "redShiftTmpFiles";
  public transient DefaultOutputPort output = new DefaultOutputPort();

  public HDFSWriteOperator()
  {
    filePath = "";
    outputFileName = "redshift-compaction";
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    filePath = context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + recoveryPath;
    super.setup(context);
  }

  @Override
  protected void finalizeFile(String fileName) throws IOException
  {
    super.finalizeFile(fileName);

    String srcPath = filePath + Path.SEPARATOR + fileName;

    output.emit(srcPath);
  }
}
