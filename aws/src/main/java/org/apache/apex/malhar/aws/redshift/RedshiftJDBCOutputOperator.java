package org.apache.apex.malhar.aws.redshift;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.Min;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.db.AbstractPassThruTransactionableStoreOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;

@OperatorAnnotation(partitionable = false)
public class RedshiftJDBCOutputOperator extends AbstractPassThruTransactionableStoreOutputOperator<String,JdbcTransactionalStore>
{
  private String tableName;
  private String bucketName;
  private String accessKey;
  private String secretKey;
  private String redshiftDelimiter;
  private String emrClusterId;
  private boolean readFromS3 = true;
  protected transient Statement stmt;
  protected static int DEFAULT_BATCH_SIZE = 100;

  @Min(1)
  private int batchSize;
  private final List<String> tuples;

  private transient int batchStartIdx;

  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<String> error = new DefaultOutputPort<>();

  @AutoMetric
  private int tuplesWrittenSuccessfully;
  @AutoMetric
  private int errorTuples;

  public RedshiftJDBCOutputOperator()
  {
    tuples = new ArrayList<>();
    batchSize = DEFAULT_BATCH_SIZE;
    batchStartIdx = 0;
    store = new JdbcTransactionalStore();
  }

  protected String generateCopyStatement(String file)
  {
    StringBuilder exec = new StringBuilder();
    exec.append("COPY " + tableName + " ");
    if (readFromS3) {
      exec.append("FROM 's3://" + bucketName + "/" + file + "' ");
    } else {
      exec.append("FROM 'emr://" + emrClusterId + "/" + file + "' ");
    }
    exec.append("CREDENTIALS 'aws_access_key_id=" + accessKey);
    exec.append(";aws_secret_access_key=" + secretKey + "' ");
    exec.append("DELIMITER '" + redshiftDelimiter + "'");
    exec.append(";");
    return exec.toString();
  }

  public void processTuple(String file)
  {
    tuples.add(file);
    if ((tuples.size() - batchStartIdx) >= batchSize) {
      processBatch();
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    tuplesWrittenSuccessfully = 0;
    errorTuples = 0;
  }

  @Override
  public void endWindow()
  {
    if (tuples.size() - batchStartIdx > 0) {
      processBatch();
    }
    super.endWindow();
    tuples.clear();
    batchStartIdx = 0;
  }

  private void processBatch()
  {
    //logger.debug("start {} end {}", batchStartIdx, tuples.size());
    try {
      for (int i = batchStartIdx; i < tuples.size(); i++) {
        stmt.addBatch(generateCopyStatement(tuples.get(i)));
      }
      stmt.executeBatch();
      stmt.clearBatch();
      batchStartIdx += tuples.size() - batchStartIdx;
    } catch (BatchUpdateException bue) {
      //logger.error(bue.getMessage());
      processUpdateCounts(bue.getUpdateCounts(), tuples.size() - batchStartIdx);
    } catch (SQLException e) {
      throw new RuntimeException("processing batch", e);
    }
  }

  /**
   * Identify which commands in the batch failed and redirect these on the error port.
   * See https://docs.oracle.com/javase/7/docs/api/java/sql/BatchUpdateException.html for more details
   *
   * @param updateCounts
   * @param commandsInBatch
   */
  private void processUpdateCounts(int[] updateCounts, int commandsInBatch)
  {
    if (updateCounts.length < commandsInBatch) {
      // Driver chose not to continue processing after failure.
      error.emit(tuples.get(updateCounts.length + batchStartIdx));
      errorTuples++;
      // In this case, updateCounts is the number of successful queries
      tuplesWrittenSuccessfully += updateCounts.length;
      // Skip the error record
      batchStartIdx += updateCounts.length + 1;
      // And process the remaining if any
      if ((tuples.size() - batchStartIdx) > 0) {
        processBatch();
      }
    } else {
      // Driver processed all batch statements in spite of failures.
      // Pick out the failures and send on error port.
      tuplesWrittenSuccessfully = commandsInBatch;
      for (int i = 0; i < commandsInBatch; i++) {
        if (updateCounts[i] == Statement.EXECUTE_FAILED) {
          error.emit(tuples.get(i + batchStartIdx));
          errorTuples++;
          tuplesWrittenSuccessfully--;
        }
      }
    }
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    try {
      stmt = store.getConnection().createStatement();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void teardown()
  {
    store.disconnect();
  }

  public String getTableName()
  {
    return tableName;
  }

  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  public String getAccessKey()
  {
    return accessKey;
  }

  public void setAccessKey(String accessKey)
  {
    this.accessKey = accessKey;
  }

  public String getSecretKey()
  {
    return secretKey;
  }

  public void setSecretKey(String secretKey)
  {
    this.secretKey = secretKey;
  }

  public String getRedshiftDelimiter()
  {
    return redshiftDelimiter;
  }

  public void setRedshiftDelimiter(String redshiftDelimiter)
  {
    this.redshiftDelimiter = redshiftDelimiter;
  }

  public String getEmrClusterId()
  {
    return emrClusterId;
  }

  public void setEmrClusterId(String emrClusterId)
  {
    this.emrClusterId = emrClusterId;
  }

  public boolean isReadFromS3()
  {
    return readFromS3;
  }

  public void setReadFromS3(boolean readFromS3)
  {
    this.readFromS3 = readFromS3;
  }

  public String getBucketName()
  {
    return bucketName;
  }

  public void setBucketName(String bucketName)
  {
    this.bucketName = bucketName;
  }
}
