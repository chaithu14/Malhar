package com.datatorrent.contrib.enrichment;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.lib.db.cache.CacheManager;
import com.datatorrent.lib.db.cache.CacheStore;
import com.datatorrent.lib.db.cache.CacheStore.ExpiryType;
import com.esotericsoftware.kryo.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Base class for Enrichment Operator.&nbsp; Subclasses should provide implementation to getKey and convert.
 * The operator receives a tuple and emits enriched tuple based on includeFields and lookupFields. <br/>
 *
 * Properties:<br>
 * <b>lookupFieldsStr</b>: List of comma seperated keys for quick searching. Ex: Field1,Field2,Field3<br>
 * <b>includeFieldsStr</b>: List of comma seperated fields to be replaced/added to the input tuple. Ex: Field1,Field2,Field3<br>
 * <b>store</b>: Specify the type of loader for looking data<br>
 * <br>
 *
 *
 * @displayName Abstract Enrichment Operator
 * @tags Enrichment
 * @param <INPUT> Type of tuples which are received by this operator</T>
 * @param <OUTPUT> Type of tuples which are emitted by this operator</T>
 * @since 2.1.0
 */
public abstract class AbstractEnrichmentOperator<INPUT, OUTPUT> extends BaseOperator
{
  /**
   * Keep lookup data cache for fast access.
   */
  private transient CacheManager cacheManager;

  private transient CacheStore primaryCache = new CacheStore();

  public transient DefaultOutputPort<OUTPUT> output = new DefaultOutputPort<OUTPUT>();

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<INPUT> input = new DefaultInputPort<INPUT>()
  {
    @Override public void process(INPUT tuple)
    {
      processTuple(tuple);
    }
  };
  private EnrichmentBackup store;

  @NotNull
  protected String lookupFieldsStr;

  protected String includeFieldsStr;

  protected transient List<String> lookupFields = new ArrayList<String>();
  protected transient List<String> includeFields = new ArrayList<String>();

  protected void processTuple(INPUT tuple) {
    Object result = cacheManager.get(getKey(tuple));
    OUTPUT out = convert(tuple, result);
    emitTuple(out);
  }

  protected abstract Object getKey(INPUT tuple);

  protected void emitTuple(OUTPUT tuple) {
    output.emit(tuple);
  }

  /* Add data from cached value to input field */
  protected abstract OUTPUT convert(INPUT in, Object cached);

  @Override public void setup(Context.OperatorContext context)
  {
    super.setup(context);

    cacheManager = new NullValuesCacheManager();

    // set expiration to one day.
    primaryCache.setEntryExpiryDurationInMillis(24 * 60 * 60 * 1000);
    primaryCache.setCacheCleanupInMillis(24 * 60 * 60 * 1000);
    primaryCache.setEntryExpiryStrategy(ExpiryType.EXPIRE_AFTER_WRITE);
    primaryCache.setMaxCacheSize(16 * 1024 * 1024);

    lookupFields = Arrays.asList(lookupFieldsStr.split(","));
    if (includeFieldsStr != null) {
      includeFields = Arrays.asList(includeFieldsStr.split(","));
    }

    try {
      store.setFields(includeFields, lookupFields);

      cacheManager.setPrimary(primaryCache);
      cacheManager.setBackup(store);
      cacheManager.initialize();
    } catch (IOException e) {
      throw new RuntimeException("Unable to initialize primary cache", e);
    }
  }

  /**
   * Set the type of backup store for storing and searching data.
   */
  public void setStore(EnrichmentBackup store) {
    this.store = store;
  }

  public EnrichmentBackup getStore() {
    return store;
  }

  public CacheStore getPrimaryCache()
  {
    return primaryCache;
  }

  public String getLookupFieldsStr()
  {
    return lookupFieldsStr;
  }

  /**
   * Set the lookup fields for quick searching. It would be in comma separated list
   */
  public void setLookupFieldsStr(String lookupFieldsStr)
  {
    this.lookupFieldsStr = lookupFieldsStr;
  }

  public String getIncludeFieldsStr()
  {
    return includeFieldsStr;
  }

  /**
   * Set the list of comma separated fields to be added/replaced to the incoming tuple.
   */
  public void setIncludeFieldsStr(String includeFieldsStr)
  {
    this.includeFieldsStr = includeFieldsStr;
  }
}
