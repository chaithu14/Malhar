package org.apache.apex.examples.parser.managedState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.spillable.SpillableMapRocksDBImpl;
import org.apache.apex.malhar.lib.state.spillable.managed.RocksDBStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.RocksDBTTLStateStore;
import org.apache.commons.lang3.tuple.MutablePair;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

public class SpillableMapRocksDBOperator implements Operator
{
  private static final transient Logger LOG = LoggerFactory.getLogger(SpillableMapRocksDBOperator.class);
  private String basePath;
  private RocksDBStateStore store;
  private int maxInsertions = 1;
  private int currentCount = 0;
  private transient SpillableMapRocksDBImpl<Long, Long> mapRocksDB;
  public transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<>();

  public transient DefaultInputPort<MutablePair<Long, Long>> inputPort = new DefaultInputPort<MutablePair<Long, Long>>()
  {
    @Override
    public void process(MutablePair<Long, Long> integerIntegerMutablePair)
    {
      if (currentCount < maxInsertions) {
        mapRocksDB.put(integerIntegerMutablePair.getLeft(), integerIntegerMutablePair.getRight());
        if (outputPort.isConnected()) {
          String output = "Insert: ";
          output += Long.toString(integerIntegerMutablePair.getLeft());
          output += " , ";
          output += Long.toString(integerIntegerMutablePair.getRight());
          outputPort.emit(output);
        }
      } else if (currentCount >= maxInsertions) {
        String keys = "";
        for (int i = 0; i < 6000; i++) {
          Long key = Long.valueOf(i);
          Long value = mapRocksDB.get(key);
          if (value == null) {
            keys += Integer.toString(i);
          }
        }
        LOG.info("current Count: {} -> {}", currentCount, keys);
        if (outputPort.isConnected() && keys.length() > 1) {
          String output = "Fetch: ";
          //output += Integer.toString(integerIntegerMutablePair.getLeft());
          output += keys;
          output += " , ";
          /*if (value != null) {
            output += Integer.toString(value);
          } else {
            output += " ";
          }*/
          outputPort.emit(output);
        }
      }
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    //store = new RocksDBStateStore();
    store = new RocksDBTTLStateStore();
    store.setDbPath(basePath);
    ((RocksDBTTLStateStore)store).setExpiryInSeconds(3600);
    mapRocksDB = new SpillableMapRocksDBImpl<>(store);
    mapRocksDB.setup(context);
  }

  @Override
  public void beginWindow(long l)
  {
    currentCount++;
    mapRocksDB.beginWindow(l);
  }

  @Override
  public void endWindow()
  {
    mapRocksDB.endWindow();
  }

  @Override
  public void teardown()
  {
    mapRocksDB.teardown();
  }

  public String getBasePath()
  {
    return basePath;
  }

  public void setBasePath(String basePath)
  {
    this.basePath = basePath;
  }

  public int getMaxInsertions()
  {
    return maxInsertions;
  }

  public void setMaxInsertions(int maxInsertions)
  {
    this.maxInsertions = maxInsertions;
  }
}
