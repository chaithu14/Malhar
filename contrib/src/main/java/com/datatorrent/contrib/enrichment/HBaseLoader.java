package com.datatorrent.contrib.enrichment;

import com.datatorrent.contrib.hbase.HBaseStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

public class HBaseLoader extends DBLoader
{

  protected final HBaseStore store;

  protected transient HTable hTable;
  public HBaseLoader() {
    store = new HBaseStore();
  }

  @Override
  public void connect() throws IOException {
    store.connect();
    hTable = store.getTable();
    if(hTable == null) {
      logger.error("HBase connection Failure");
    }
  }

  @Override protected Object getQueryResult(Object key)
  {
    try {
      Get get = new Get(getRowBytes(key));
      for(String f : includeFields) {
        get.addFamily(Bytes.toBytes(f));
      }
      return hTable.get(get);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  @Override protected ArrayList<Object> getDataFrmResult(Object result)
  {
    Result res = (Result)result;
    if (res == null || res.isEmpty())
      return null;
    ArrayList<Object> columnInfo = new ArrayList<Object>();

    for(KeyValue kv : res.raw()) {
      columnInfo.add(kv.getValue());
    }
    return columnInfo;
  }

  private byte[] getRowBytes(Object key)
  {
    return ((String)key).getBytes();
  }

  @Override
  public void disconnect() throws IOException
  {
    store.disconnect();
  }

  @Override
  public boolean isConnected() {
    return store.isConnected();
  }


  public void setZookeeperQuorum(String zookeeperQuorum) {
    store.setZookeeperQuorum(zookeeperQuorum);
  }
  public void setZookeeperClientPort(int zookeeperClientPort) {
    store.setZookeeperClientPort(zookeeperClientPort);
  }
  public void setTableName(String tableName) {
    store.setTableName(tableName);
  }
  public void setPrincipal(String principal)
  {
    store.setPrincipal(principal);
  }
  public void setKeytabPath(String keytabPath)
  {
    store.setKeytabPath(keytabPath);
  }

  public void setReloginCheckInterval(long reloginCheckInterval)
  {
    store.setReloginCheckInterval(reloginCheckInterval);
  }
}
