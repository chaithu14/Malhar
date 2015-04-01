package com.datatorrent.contrib.enrichment;

import com.datatorrent.lib.db.Connectable;
import com.datatorrent.lib.db.jdbc.JdbcStore;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class DBLoader extends ReadOnlyBackup {
  protected static final Logger logger = LoggerFactory.getLogger(DBLoader.class);

  protected abstract Object getQueryResult(Object key);
  protected abstract ArrayList<Object> getDataFrmResult(Object stmt);

  @Override
  public Map<Object, Object> loadInitialData() {
    return null;
  }

  @Override
  public Object get(Object key) {
    return getDataFrmResult(getQueryResult(key));
  }

  @Override
  public List<Object> getAll(List<Object> keys) {
    List<Object> values = Lists.newArrayList();
    for (Object key : keys) {
      values.add(get(key));
    }
    return values;
  }

  @Override
  public void disconnect() throws IOException {
  }

  @Override
  public boolean isConnected() {
    return false;
  }
}
