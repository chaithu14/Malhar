package org.apache.apex.malhar.aws.redshift;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.apex.malhar.lib.fs.s3.S3CompactionOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;

public class RedshiftOutputModule implements Module
{
  private String tableName;
  private String bucketName;
  private String accessKey;
  private String secretKey;
  private String redshiftDelimiter;
  private String emrClusterId;
  private Long maxLength;
  private String directoryName;
  private boolean readFromS3 = true;

  public final transient ProxyInputPort<byte[]> input = new ProxyInputPort<byte[]>();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    if (readFromS3) {
      S3CompactionOperator<byte[]> s3compaction = dag.addOperator("S3Compaction", new S3CompactionOperator());
      s3compaction.setConverter(new GenericFileOutputOperator.NoOpConverter());
      if (maxLength != null) {
        s3compaction.setMaxLength(maxLength);
      }

      S3OutputOperator s3Reconciler = dag.addOperator("S3Reconciler", new S3OutputOperator());
      s3Reconciler.setAccessKey(accessKey);
      s3Reconciler.setSecretKey(secretKey);
      s3Reconciler.setBucketName(bucketName);
      s3Reconciler.setDirectoryName(directoryName);
      input.set(s3compaction.input);

      RedshiftJDBCOutputOperator redshiftOutput = dag.addOperator("LoadToRedshift", new RedshiftJDBCOutputOperator());
      redshiftOutput.setAccessKey(accessKey);
      redshiftOutput.setSecretKey(secretKey);
      redshiftOutput.setBucketName(bucketName);
      redshiftOutput.setTableName(tableName);
      redshiftOutput.setEmrClusterId(emrClusterId);
      redshiftOutput.setReadFromS3(readFromS3);

      dag.addStream("write-to-s3", s3compaction.output, s3Reconciler.input);
      dag.addStream("load-to-redshift", s3Reconciler.outputPort, redshiftOutput.input);
    } else {
      HDFSWriteOperator<byte[]> hdfsWriteOperator = dag.addOperator("WriteToHDFS", new HDFSWriteOperator<byte[]>());
      hdfsWriteOperator.setConverter(new GenericFileOutputOperator.NoOpConverter());
      if (maxLength != null) {
        hdfsWriteOperator.setMaxLength(maxLength);
      }
      input.set(hdfsWriteOperator.input);

      RedshiftJDBCOutputOperator redshiftOutput = dag.addOperator("LoadToRedshift", new RedshiftJDBCOutputOperator());
      redshiftOutput.setAccessKey(accessKey);
      redshiftOutput.setSecretKey(secretKey);
      redshiftOutput.setBucketName(bucketName);
      redshiftOutput.setTableName(tableName);
      redshiftOutput.setEmrClusterId(emrClusterId);
      redshiftOutput.setReadFromS3(readFromS3);

      dag.addStream("load-to-redshift", hdfsWriteOperator.output, redshiftOutput.input);
    }
  }
}
