package org.apache.apex.malhar.aws.redshift;

import org.apache.apex.malhar.lib.fs.s3.S3Reconciler;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.DefaultOutputPort;

public class S3OutputOperator extends S3Reconciler
{
  public final transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<>();

  @Override
  public void removeProcessedTuple(OutputMetaData metaData)
  {
    super.removeProcessedTuple(metaData);
    outputPort.emit(getDirectoryName() + Path.SEPARATOR + metaData.getFileName());
  }
}
