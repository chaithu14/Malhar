package com.datatorrent.contrib.enrichment;

import com.datatorrent.common.util.DTThrowable;

import com.datatorrent.lib.db.jdbc.JdbcStore;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.validation.constraints.NotNull;

public class JDBCLoader extends DBLoader
{
  protected transient Connection connection = null;

  protected final JdbcStore store;

  protected String queryStmt;

  protected String tableName;

  public JDBLoader() {
    store = new JdbcStore();
  }

  @Override
  public void connect() throws IOException {
    store.connect();
    connection = store.getConnection();
    if(connection == null) {
      logger.error("JDBC connection Failure");
    }
  }

  @Override protected Object getQueryResult(Object key)
  {
    try {
      PreparedStatement getStatement ;
      if(queryStmt == null) {
        getStatement = connection.prepareStatement(generateQueryStmt(key));
      } else {
        getStatement = connection.prepareStatement(queryStmt);
        ArrayList<Object> keys = (ArrayList<Object>) key;
        for (int i = 0; i < keys.size(); i++) {
          getStatement.setObject(i+1, keys.get(i));
        }
      }
      return getStatement.executeQuery();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override protected ArrayList<Object> getDataFrmResult(Object result) throws RuntimeException
  {
    try {
      ResultSet resultSet = (ResultSet) result;
      if (resultSet.next()) {
        ArrayList<Object> res = new ArrayList<Object>();
        if(queryStmt == null) {
          for(String f : includeFields) {
            res.add(resultSet.getObject(f));
          }
        } else {
          ResultSetMetaData rsdata = resultSet.getMetaData();
          int columnCount = rsdata.getColumnCount();
          // If the includefields is empty, populate it from ResultSetMetaData
          if(includeFields == null || includeFields.size() == 0) {
            if(includeFields == null)
              includeFields = new ArrayList<String>();
            for (int i = 1; i <= columnCount; i++) {
              includeFields.add(rsdata.getColumnName(i));
            }
          }

          for (int i = 1; i <= columnCount; i++) {
            res.add(resultSet.getObject(i));
          }
        }
        return res;
      } else
        return null;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private String generateQueryStmt(Object key)
  {
    String stmt = "select * from " + tableName + " where ";
    ArrayList<Object> keys = (ArrayList<Object>) key;
    for (int i = 0; i < keys.size(); i++) {
      stmt = stmt + lookupFields.get(i) + " = " + keys.get(i);
      if(i != keys.size() - 1) {
        stmt = stmt + " and ";
      }
    }
    return stmt;
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

  public String getQueryStmt()
  {
    return queryStmt;
  }

<<<<<<< HEAD:contrib/src/main/java/com/datatorrent/contrib/enrichment/JDBCLoader.java
  @Override
  public boolean needRefresh() {
    return false;
  }

  public void disconnect() throws IOException
=======
  public void setQueryStmt(String queryStmt)
>>>>>>> Changed the interface DBLoader and JDBLoader:contrib/src/main/java/com/datatorrent/contrib/enrichment/JDBLoader.java
  {
    this.queryStmt = queryStmt;
  }

  public String getTableName()
  {
    return tableName;
  }

  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  public void setDbUrl(String dbUrl)
  {
    store.setDbUrl(dbUrl);
  }

  public void setDbDriver(String dbDriver)
  {
    store.setDbDriver(dbDriver);
  }

  public void setUserName(String userName)
  {
    store.setUserName(userName);
  }

  public void setPassword(String password)
  {
    store.setPassword(password);
  }

  public void setConnectionProperties(String connectionProperties)
  {
    store.setConnectionProperties(connectionProperties);
  }
}
