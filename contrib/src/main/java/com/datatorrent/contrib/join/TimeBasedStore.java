package com.datatorrent.contrib.join;

import com.datatorrent.api.Context;
import com.datatorrent.lib.bucket.Bucketable;
import com.datatorrent.lib.bucket.Event;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import javax.validation.constraints.Min;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeBasedStore<T extends Event & Bucketable>
{
  private static transient final Logger logger = LoggerFactory.getLogger(TimeBasedStore.class);
  @Min(1)
  protected int noOfBuckets;
  protected TimeBucket<T>[] buckets;
  protected long expiryTimeInMillis = 1;
  protected long spanTime = 1;
  protected int bucketSpanInMillis = 30000;
  protected long startOfBucketsInMillis;
  protected long endOBucketsInMillis;
  private final transient Lock lock;
  private transient Map<Object, List<Long>> key2Buckets;
  private transient Timer bucketSlidingTimer;
  private transient Timer bucketMergeTimer;
  private transient long bucketMergeSpanInMillis = 23000;
  private long expiryTime;
  private String bucketRoot;
  private transient Kryo writeSerde;
  private transient long mergeBucketId = 0L;

  //@NotNull
  //protected final Map<Integer, TimeBucket<T>> dirtyBuckets;
  static transient final String PATH_SEPARATOR = "/";
  protected transient Configuration configuration;
  protected Long latestBucketId = 0L;
  protected Long currentBucketId = 0L;
  protected transient Map<Long, DTFileReader> readers = new HashMap<Long, DTFileReader>();

  protected Map<Long, TimeBucket> expiredBuckets;

  public void setBucketRoot(String bucketRoot)
  {
    this.bucketRoot = bucketRoot;
  }

  public TimeBasedStore()
  {
    lock = new Lock();
    expiredBuckets = new HashMap<Long, TimeBucket>();
    key2Buckets = new HashMap<Object, List<Long>>();
    //dirtyBuckets = new HashMap<Integer, TimeBucket<T>>();
  }

  private void recomputeNumBuckets()
  {
    Calendar calendar = Calendar.getInstance();
    long now = calendar.getTimeInMillis();
    startOfBucketsInMillis = now - spanTime;
    expiryTime = startOfBucketsInMillis;
    expiryTimeInMillis = startOfBucketsInMillis;
    endOBucketsInMillis = now;
    noOfBuckets = (int) Math.ceil((now - startOfBucketsInMillis) / (bucketSpanInMillis * 1.0));
    buckets = (TimeBucket<T>[]) Array.newInstance(TimeBucket.class, noOfBuckets);
  }

  public void setup(Context.OperatorContext context)
  {
    configuration = new Configuration();
    recomputeNumBuckets();
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    this.writeSerde = new Kryo();
    writeSerde.setClassLoader(classLoader);
    startMergeService();
    startService();

  }

  public void setExpiryTime(long expiryTime)
  {
    this.expiryTime = expiryTime;
  }

  public Object getValidTuples(T tuple)
  {
    //logger.info("GetValidTuples: " + tuple);
    Object key = tuple.getEventKey();
    Kryo kryo = new Kryo();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output output2 = new Output(bos);
    kryo.writeObject(output2, key);
    output2.close();
    byte[] keyBytes = bos.toByteArray();

    if (key2Buckets.get(key) == null) {
      //logger.info("GetValidTuples - 1: " + tuple);
      return null;
    }
    List<Long> keyBuckets = new ArrayList<Long>(key2Buckets.get(key));
    long start = System.currentTimeMillis();
    List<Event> validTuples = new ArrayList<Event>();
    //logger.info("GetValidTuples - 2: " + tuple);
    for (Long idx : keyBuckets) {
      //logger.info("GetValidTuples - 3: " + tuple);
      if (expiredBuckets.get(idx) != null) {
        //logger.info("GetValidTuples - 4: " + tuple);
        TimeBucket tb = (TimeBucket) expiredBuckets.get(idx);
        List<T> events = getDataFromFile(keyBytes, tb.bucketKey);
        if (events != null) {
          //logger.info("GetValidTuples - 5: " + tuple);
          for (T event : events) {
            if (Math.abs(tuple.getTime() - event.getTime()) < spanTime) {
              validTuples.add(event);
            }
          }
        }
      } else {
        //logger.info("GetValidTuples - 8: " + tuple);
        int bucketIdx = (int) (idx % noOfBuckets);
        TimeBucket tb = (TimeBucket) buckets[bucketIdx];
        if (tb == null) {
          //logger.info("GetValidTuples - 9: " + tuple);
          continue;
        }
        List<T> events = tb.get(key);
        if (tb.isDataOnDiskLoaded()) {
          List<T> dataEvents = getDataFromFile(keyBytes, tb.bucketKey);
          if (dataEvents != null) {
            validTuples.addAll(dataEvents);
          }
        }
        if (events != null) {
          validTuples.addAll(events);
        }
      }
    }
    //logger.info("Time took for {} Valid Tuples: {}", validTuples.size(), System.currentTimeMillis() - start);
    return validTuples;
  }

  private void updateBuckets(long time)
  {
    if (time < endOBucketsInMillis) {
      return;
    }
    int count = (int) ((time - endOBucketsInMillis) / (bucketSpanInMillis * 1.0));
    for (int i = 0; i < count; i++) {
      TimeBucket<T> b = buckets[i];
      if (b != null) {
        logger.info("updateBuckets:  {}  --- {} -> {} -> {}", b.bucketKey, time, endOBucketsInMillis, bucketRoot);
        expiredBuckets.put(b.bucketKey, b);
        buckets[i] = null;
      }
    }
  }

  public void put(T tuple)
  {
    long bucketKey = getBucketKeyFor(tuple);
    newEvent(bucketKey, tuple);
  }

  public long getBucketKeyFor(T event)
  {
    long eventTime = getTime(event);
    if (eventTime < expiryTimeInMillis) {
      return -1;
    }
    long diffFromStart = eventTime - startOfBucketsInMillis;
    long key = diffFromStart / bucketSpanInMillis;

    synchronized (lock) {
      if (eventTime > endOBucketsInMillis) {
        long move = ((eventTime - endOBucketsInMillis) / bucketSpanInMillis + 1) * bucketSpanInMillis;
        expiryTimeInMillis += move;
        endOBucketsInMillis += move;
      }
    }
    return key;
  }

  protected long getTime(T event)
  {
    return event.getTime();
  }

  private static class Lock
  {
  }

  public void newEvent(long bucketKey, T event)
  {
    int bucketIdx = (int) (bucketKey % noOfBuckets);

    //logger.info("Bucket IDx: {}", bucketIdx);
    TimeBucket<T> bucket = buckets[bucketIdx];

    if (bucket == null || bucket.bucketKey != bucketKey) {
      if (bucket != null) {
        //logger.info("NewEvent:    --- {}", bucket.bucketKey);
        expiredBuckets.put(bucket.bucketKey, bucket);
      }
      if (latestBucketId.equals(0L)) {
        latestBucketId = bucketKey;
      }
      currentBucketId = bucketKey;
      bucket = createBucket(bucketKey);
      buckets[bucketIdx] = bucket;
    }
      /*dirtyBuckets.put(bucketIdx, bucket);
      if(dirtyBuckets.size() == 0) {
        latestBucketId = bucketKey;
      }
    } else if (dirtyBuckets.get(bucketIdx) == null) {
      dirtyBuckets.put(bucketIdx, bucket);
    }*/

    Object key = bucket.getEventKey(event);
    List<Long> keyBuckets = key2Buckets.get(key);
    if (keyBuckets == null) {
      key2Buckets.put(key, Lists.newArrayList(bucketKey));
    } else {
      key2Buckets.get(key).add(bucketKey);
    }
    bucket.addNewEvent(bucket.getEventKey(event), event);
    //logger.info("Bucket IDx: {}", bucketIdx);
  }

  public void startService()
  {
    bucketSlidingTimer = new Timer();
    endOBucketsInMillis = expiryTime + (noOfBuckets * bucketSpanInMillis);
    logger.debug("bucket properties {}, {}", spanTime, bucketSpanInMillis);
    logger.debug("bucket time params: start {}, end {}", startOfBucketsInMillis, endOBucketsInMillis);

    bucketSlidingTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        long time = 0;

        synchronized (lock) {
          time = (expiryTime += bucketSpanInMillis);
          endOBucketsInMillis += bucketSpanInMillis;

        }

        try {
          deleteExpiredBuckets(time);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

    }, bucketSpanInMillis, bucketSpanInMillis);
  }

  private void deleteExpiredBuckets(long time) throws IOException
  {
    logger.info("DeleteExpiredB: File: {}, time: {}", bucketRoot, time);
    Iterator<Long> exIterator = expiredBuckets.keySet().iterator();
    for (; exIterator.hasNext(); ) {
      long key = exIterator.next();
      TimeBucket t = (TimeBucket) expiredBuckets.get(key);
      logger.info("DeleteExpiredB - 1: File: {}, time: {}", bucketRoot, time);
      if (startOfBucketsInMillis + (t.bucketKey * noOfBuckets) < time) {
        logger.info("DeleteExpiredB - 2: File: {}, time: {},  key: {}", bucketRoot, time, t.bucketKey);
        deleteBucket(t);
        exIterator.remove();
      }
    }
    //expiredBuckets.clear();
  }

  private void deleteBucket(TimeBucket bucket)
  {
    if (bucket == null) {
      return;
    }
    String bucketPath = null;
    try {
      DTFileReader bcktReader = readers.remove(bucket.bucketKey);
      if (bcktReader != null) {
        bucketPath = bcktReader.getPath();
        bcktReader.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (bucketPath == null) {
      return;
    }
    Path dataFilePath = new Path(bucketPath);
    FileSystem fs = null;
    try {
      fs = FileSystem.newInstance(dataFilePath.toUri(), configuration);
      if (fs.exists(dataFilePath)) {
        logger.debug("start delete {}", bucket.bucketKey);
        fs.delete(dataFilePath, true);
        logger.debug("end delete {}", bucket.bucketKey);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        fs.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    Map<Object, List<T>> writtens = bucket.getEvents();
    if (writtens == null) {
      return;
    }
    for (Map.Entry<Object, List<T>> e : writtens.entrySet()) {
      key2Buckets.get(e.getKey()).remove(bucket.bucketKey);
      if (key2Buckets.get(e.getKey()).size() == 0) {
        key2Buckets.remove(e.getKey());
      }
    }
  }

  public void endWindow(long window)
  {
    if (mergeBucketId != 0L) {
      TimeBucket bucket = buckets[((int) (mergeBucketId % noOfBuckets))];
      bucket.transferDataFromMemoryToStore();

      String merge = "";
      DTFileReader bcktReader = readers.get(mergeBucketId);
      String readerPath = null;
      if(bcktReader != null)
        readerPath = bcktReader.getPath();
      if (readerPath != null) {
        if (!readerPath.endsWith("_MERGE")) {
          merge = "_MERGE";
        }
      }
      String path = new String(bucketRoot + PATH_SEPARATOR + ((TimeBucket) bucket).bucketKey + merge);
      DTFileReader tr = createDTReader(path);
      if (tr != null) {
        readers.put(((TimeBucket) bucket).bucketKey, tr);
        if (bcktReader != null) {
          try {
            bcktReader.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
      if (readerPath != null) {
        Path dataFilePath = new Path(readerPath);
        FileSystem fs = null;
        try {
          fs = FileSystem.newInstance(dataFilePath.toUri(), configuration);
          if (fs.exists(dataFilePath)) {
            logger.debug("start delete {}", bucket.bucketKey);
            fs.delete(dataFilePath, true);
            logger.debug("end delete {}", bucket.bucketKey);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        } finally {
          try {
            fs.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
      mergeBucketId = 0L;
    }

    /*if(!latestBucketId.equals(0)) {
      int bucketIdx = (int) (latestBucketId % noOfBuckets);
      TimeBucket<T> bucket = buckets[bucketIdx];
      if(bucket == null || bucket.getEvents() == null || bucket.getEvents().isEmpty()) {
        return;
      }
      //logger.info("-------------Saving Data: ----------: {} -> {} ", latestBucketId);
      bucket.transferEvents();
    }*/
}

  protected void saveData() throws IOException
  {
    int bucketIdx = (int) (latestBucketId % noOfBuckets);
    TimeBucket<T> bucket = buckets[bucketIdx];
    if(bucket == null || bucket.getEvents() == null || bucket.getEvents().isEmpty()) {
      return;
    }
    logger.info("-------------Saving Data: ----------: {} -> {} ", latestBucketId);
    bucket.transferEvents();
    Map<Object, List<T>> events = new HashMap<Object, List<T>>(bucket.getWrittenEvents());
    long bucketKey = bucket.bucketKey;
    logger.info("-------------Saving Data - 1: ----------: {} -> {} ", latestBucketId);
    DTFileReader bcktReader = readers.get(bucketKey);
    TreeMap<byte[], byte[]> storedData = null;
    String readerPath = null;
    logger.info("-------------Saving Data - 2: ----------: {} -> {} ", latestBucketId);
    if(bcktReader != null) {
      //storedData = bcktReader.readFully();
      readerPath = bcktReader.getPath();
      DTFileReader br = createDTReader(readerPath);
      storedData = br.readFully();
      br.close();
    }
    logger.info("-------------Saving Data - 3: ----------: {} -> {} ", latestBucketId);
    int eventsCount = 0;
    if(events != null)
      eventsCount = events.size();
    long start = System.currentTimeMillis();
    logger.info("start store {}", latestBucketId);
    logger.info("-------------Saving Data - 4: ----------: {} -> {} ", latestBucketId);
    storeBucketData(events, storedData, bucketKey, readerPath);
    logger.info("end store {} num {} took {}", latestBucketId, eventsCount, System.currentTimeMillis() - start);
    mergeBucketId = bucketKey;
    events.clear();
    /*bucket.transferDataFromMemoryToStore();

    String merge = "";
    if(readerPath != null) {
      if(!readerPath.endsWith("_MERGE")) {
        merge = "_MERGE";
      }
    }
    String path = new String(bucketRoot + PATH_SEPARATOR + ((TimeBucket) bucket).bucketKey + merge);
    DTFileReader tr = createDTReader(path);
    if(tr != null) {
      synchronized (lock) {
        readers.put(((TimeBucket) bucket).bucketKey, tr);
      }
      if(bcktReader != null) {
        bcktReader.close();
      }
    }*/
  }

  public void startMergeService()
  {
    bucketMergeTimer = new Timer();

    logger.info("Merge bucket time : ");

    bucketMergeTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        long start = System.currentTimeMillis();
        logger.info("run Merge Service------------------- {} ");
        int bucketIdx = (int) (latestBucketId % noOfBuckets);
        if(buckets[bucketIdx].getEvents() == null && !currentBucketId.equals(latestBucketId)) {
          latestBucketId = currentBucketId;
        }
        try {
          logger.info("-------------Saving Data: ----------: {} ", latestBucketId);
          saveData();
          logger.info("-------------End of Saving Data: ----------: {} -> {}", latestBucketId, System.currentTimeMillis() - start);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

    }, bucketMergeSpanInMillis, bucketMergeSpanInMillis);
  }
  private void setupConfig(Configuration conf)
  {
    int chunkSize = 1024 * 1024;

    int inputBufferSize = 256 * 1024;

    int outputBufferSize = 256 * 1024;
    conf.set("tfile.io.chunk.size", String.valueOf(chunkSize));
    conf.set("tfile.fs.input.buffer.size", String.valueOf(inputBufferSize));
    conf.set("tfile.fs.output.buffer.size", String.valueOf(outputBufferSize));
  }

  public void storeBucketData(Map<Object, List<T>> bucketData, TreeMap<byte[], byte[]> storedData, long bucketKey, String basePath)
  {
    logger.info("BucketData: {}", bucketRoot);
    TreeMap<byte[], byte[]> sortedData = new TreeMap<byte[], byte[]>(new Comparator<byte[]>()
    {
      @Override public int compare(byte[] bytes, byte[] bytes2)
      {
        int end1 = bytes.length;
        int end2 = bytes2.length;
        for (int i = 0, j = 0; i < end1 && j < end2; i++, j++) {
          int a = (bytes[i] & 0xff);
          int b = (bytes2[j] & 0xff);
          if (a != b) {
            return a - b;
          }
        }
        return end1 - end2;
      }
    });

    logger.info("-------------StoreBucketData - 1: ----------: {} ", latestBucketId);
    //Write the size of data and then data
    //dataStream.writeInt(bucketData.size());
    Kryo kryo = new Kryo();
    for (Map.Entry<Object, List<T>> entry : bucketData.entrySet()) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Output output2 = new Output(bos);
      kryo.writeObject(output2, entry.getKey());
      output2.close();
      ByteArrayOutputStream bos1 = new ByteArrayOutputStream();
      Output output1 = new Output(bos1);
      kryo.writeObject(output1, entry.getValue());
      output1.close();
      sortedData.put(bos.toByteArray(), bos1.toByteArray());
    }

    logger.info("-------------StoreBucketData - 2: ----------: {} ", latestBucketId);

    String merge = "";
    if(basePath != null) {
      if(storedData != null) {
        sortedData.putAll(storedData);
      }
      if(!basePath.endsWith("_MERGE")) {
        merge = "_MERGE";
      }
    }
    logger.info("-------------StoreBucketData - 3: ----------: {} ", latestBucketId);
    Path dataFilePath = new Path(bucketRoot + PATH_SEPARATOR + bucketKey + merge);
    FSDataOutputStream dataStream = null;
    FileSystem fs = null;
    TFile.Writer writer = null;
    try {
      fs = FileSystem.newInstance(dataFilePath.toUri(), configuration);
      dataStream = fs.create(dataFilePath);
      logger.info("data FilePath: {}", dataFilePath.getName());
      int minBlockSize = 64 * 1024;

      String compressName = TFile.COMPRESSION_NONE;

      String comparator = "memcmp";

      setupConfig(fs.getConf());
      writer  = new TFile.Writer(dataStream, minBlockSize, compressName, comparator, configuration);
      for(Map.Entry<byte[], byte[]> entry : sortedData.entrySet()) {
        //logger.info("----------Key: {} ", entry.getKey());
        writer.append(entry.getKey(), entry.getValue());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if(fs != null) {
        try {
          writer.close();
          dataStream.close();
          fs.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

      }
    }

  }

  private TFileReader createReader(String path)
  {
    Path dataFile = new Path(path);
    FileSystem fs = null;
    try {
      fs = FileSystem.newInstance(dataFile.toUri(), configuration);
      if(!fs.exists(dataFile)) {
        return null;
      }
      FSDataInputStream fsdis = fs.open(dataFile);
      return new TFileReader(fsdis, fs.getFileStatus(dataFile).getLen(), configuration);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private DTFileReader createDTReader(String path)
  {
    Path dataFile = new Path(path);
    FileSystem fs = null;
    try {
      fs = FileSystem.newInstance(dataFile.toUri(), configuration);
      if(!fs.exists(dataFile)) {
        return null;
      }
      FSDataInputStream fsdis = fs.open(dataFile);
      return new DTFileReader(fsdis, fs.getFileStatus(dataFile).getLen(), configuration, path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private List<T> getDataFromFile(byte[] keyBytes, long bucketKey)
  {
    /*if(bucketKey > 1L) {
      return null;
    }*/

    DTFileReader reader = readers.get(bucketKey);
    if(reader == null)
      return null;
    byte[] value = null;
    try {
      if(reader.get(keyBytes, value))
      {
        if(value != null) {
          Input lInput = new Input(value);
          Kryo kro = new Kryo();
          return (List<T>)kro.readObject(lInput, ArrayList.class);
        }
        return null;
      }
    } catch (IOException e) {
      throw new RuntimeException("Excetpion from " + reader.getPath() + " ==>  " + e);
    }
    return null;
  }

  protected TimeBucket<T> createBucket(long bucketKey)
  {
    return new TimeBucket<T>(bucketKey);
  }

  public void setSpanTime(long spanTime)
  {
    this.spanTime = spanTime;
  }

  public void setBucketSpanInMillis(int bucketSpanInMillis)
  {
    this.bucketSpanInMillis = bucketSpanInMillis;
  }

  public void shutdown()
  {
    bucketSlidingTimer.cancel();
    bucketMergeTimer.cancel();
  }
}


