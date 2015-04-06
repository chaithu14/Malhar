package com.datatorrent.contrib.enrichment;

import com.datatorrent.common.util.DTThrowable;

import com.datatorrent.lib.db.jdbc.JdbcStore;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.validation.constraints.NotNull;

public class JDBCLoader extends JdbcStore implements EnrichmentBackup
{
  protected String queryStmt;

  protected String tableName;

  protected List<String> includeFields;
  protected List<String> lookupFields;

  protected Object getQueryResult(Object key)
  {
    try {
      PreparedStatement getStatement ;
      if(queryStmt == null) {
        getStatement = getConnection().prepareStatement(generateQueryStmt(key));
      } else {
        getStatement = getConnection().prepareStatement(queryStmt);
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

  protected ArrayList<Object> getDataFrmResult(Object result) throws RuntimeException
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

  public String getQueryStmt()
  {
    return queryStmt;
  }

  @Override
  public boolean needRefresh() {
    return false;
  }

  public void setQueryStmt(String queryStmt)
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

  @Override public void setLookupFields(List<String> lookupFields)
  {
    this.lookupFields = lookupFields;
  }

  @Override public void setIncludeFields(List<String> includeFields)
  {
    this.includeFields = includeFields;
  }

  @Override public Map<Object, Object> loadInitialData()
  {
    return null;
  }

  @Override public Object get(Object key)
  {
    return getDataFrmResult(getQueryResult(key));
  }

  @Override public List<Object> getAll(List<Object> keys)
  {
    List<Object> values = Lists.newArrayList();
    for (Object key : keys) {
      values.add(get(key));
    }
    return values;
  }

  @Override public void put(Object key, Object value)
  {
    throw new RuntimeException("Not supported operation");
  }

  @Override public void putAll(Map<Object, Object> m)
  {
    throw new RuntimeException("Not supported operation");
  }

  @Override public void remove(Object key)
  {
    throw new RuntimeException("Not supported operation");
  }
}
