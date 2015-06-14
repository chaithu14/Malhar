package com.datatorrent.contrib.join;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.lib.bucket.Bucketable;
import com.datatorrent.lib.bucket.Event;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
  protected transient TimeBucket<T>[] buckets;
  protected long expiryTimeInMillis = 1;
  protected long spanTime = 1;
  protected int bucketSpanInMillis = 30000;
  protected long startOfBucketsInMillis;
  protected long endOBucketsInMillis;
  private final transient Lock lock;
  private transient Map<Object, List<Long>> key2Buckets;
  private transient Timer bucketSlidingTimer;
  private transient Timer bucketMergeTimer;
  protected Map<Long, Long>[] bucketPositions;
  protected Map<Long, Long>[] bucketSavePositions;
  private transient long bucketMergeSpanInMillis = 23000;
  private long expiryTime;
  private String bucketRoot;
  private transient Kryo writeSerde;
  private transient long mergeBucketId = 0L;
  private Set<Integer> mergeBuckets;
  private transient Set<Integer> storeBuckets;
  private transient Class eventKeyClass;

  //@NotNull
  //protected final Map<Integer, TimeBucket<T>> dirtyBuckets;
  protected transient Set<Long> dirtyBuckets;
  static transient final String PATH_SEPARATOR = "/";
  protected transient Configuration configuration;
  protected transient Map<Long, DTFileReader> readers = new HashMap<Long, DTFileReader>();
  //protected transient Map<Long, HFileReader> readers = new HashMap<Long, HFileReader>();
  protected transient ThreadPoolExecutor threadPoolExecutor;
  protected transient ThreadPoolExecutor threadFetchExecutor;

  protected transient Map<Long, TimeBucket> expiredBuckets;

  public void setBucketRoot(String bucketRoot)
  {
    this.bucketRoot = bucketRoot;
  }

  public TimeBasedStore()
  {
    lock = new Lock();

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
    key2Buckets = new HashMap<Object, List<Long>>();
    bucketPositions = (Map<Long, Long>[]) Array.newInstance(HashMap.class, noOfBuckets);
    bucketSavePositions = (Map<Long, Long>[]) Array.newInstance(HashMap.class, noOfBuckets);
    expiredBuckets = new HashMap<Long, TimeBucket>();
  }

  public void setup(Context.OperatorContext context)
  {
    configuration = new Configuration();
    recomputeNumBuckets();
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    this.writeSerde = new Kryo();
    writeSerde.setClassLoader(classLoader);
    //startMergeService();
    startService();
    dirtyBuckets = new HashSet<Long>();
    storeBuckets = new HashSet<Integer>();

  }

  public void setExpiryTime(long expiryTime)
  {
    this.expiryTime = expiryTime;
  }

  public Object getValidTuples(T tuple)
  {
    Object key = tuple.getEventKey();
    Kryo kryo = new Kryo();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output output2 = new Output(bos);
    kryo.writeObject(output2, key);
    output2.close();
    byte[] keyBytes = bos.toByteArray();
    List<Event> validTuples = new ArrayList<Event>();

    /*for(int i = 0; i < noOfBuckets; i++) {
      TimeBucket tb = buckets[i];
      if(tb == null) {
        continue;
      }
      if(tb.contains(key)) {
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
    }*/

    if (key2Buckets.get(key) == null) {
      //logger.info("GetValidTuples - 1: " + tuple);
      return null;
    }
    List<Long> keyBuckets = new ArrayList<Long>(key2Buckets.get(key));
    long start = System.currentTimeMillis();
    for (Long idx : keyBuckets) {
      if (expiredBuckets.get(idx) != null) {
        TimeBucket tb = (TimeBucket) expiredBuckets.get(idx);
        List<T> events = getDataFromFile(keyBytes, tb.bucketKey);
        if (events != null) {
          for (T event : events) {
            if (Math.abs(tuple.getTime() - event.getTime()) < spanTime) {
              validTuples.add(event);
            }
          }
        }
      } else {
        int bucketIdx = (int) (idx % noOfBuckets);
        TimeBucket tb = (TimeBucket) buckets[bucketIdx];
        if (tb == null) {
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

    TimeBucket<T> bucket = buckets[bucketIdx];

    if (bucket == null || bucket.bucketKey != bucketKey) {
      if (bucket != null) {
        expiredBuckets.put(bucket.bucketKey, bucket);
      }
      bucket = createBucket(bucketKey);
      buckets[bucketIdx] = bucket;
    }

    Object key = bucket.getEventKey(event);
    List<Long> keyBuckets = key2Buckets.get(key);
    if (keyBuckets == null) {
      key2Buckets.put(key, Lists.newArrayList(bucketKey));
    } else {
      key2Buckets.get(key).add(bucketKey);
    }
    eventKeyClass = key.getClass();
    bucket.addNewEvent(bucket.getEventKey(event), event);
    dirtyBuckets.add(bucketKey);
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
      //HFileReader bcktReader = readers.remove(bucket.bucketKey);
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

    /*Map<Object, List<T>> writtens = bucket.getEvents();
    if (writtens == null) {
      return;
    }
    for (Map.Entry<Object, List<T>> e : writtens.entrySet()) {
      key2Buckets.get(e.getKey()).remove(bucket.bucketKey);
      if (key2Buckets.get(e.getKey()).size() == 0) {
        key2Buckets.remove(e.getKey());
      }
    }*/
  }

  public void endWindow(long window)
  {
    saveBucketData(window);
    if(mergeBuckets != null && mergeBuckets.size() != 0) {
      for(long mergeBucketId: mergeBuckets) {
        TimeBucket bucket = buckets[((int) (mergeBucketId % noOfBuckets))];
        bucket.transferDataFromMemoryToStore();

        String merge = "";
        DTFileReader bcktReader = readers.get(mergeBucketId);
        //HFileReader bcktReader = readers.get(mergeBucketId);
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
        //HFileReader tr = createHReader(path);
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
      }
      mergeBuckets = null;
    }
}

  protected void mergeData(int bucketIdx, Long bucketKey) throws IOException
  {
    Map<Long, Long> bucketPos = bucketSavePositions[bucketIdx];
    Map<Object, List<TimeEvent>> bucketDataPerWindow = Maps.newHashMap();

    for(Map.Entry<Long, Long> e : bucketPos.entrySet()) {
      Kryo readSerde = new Kryo();

      Path dataFile = new Path(bucketRoot + PATH_SEPARATOR + "Windows" + PATH_SEPARATOR + e.getKey());
      FileSystem fs = FileSystem.newInstance(dataFile.toUri(), configuration);

      try {
        //Read data only for the fileIds in which bucketIdx had events.
        FSDataInputStream stream = fs.open(dataFile);
        stream.seek(e.getValue());
        Input input = new Input(stream);

        int length = stream.readInt();

        for (int i = 0; i < length; i++) {
          Object key = readSerde.readObject(input, eventKeyClass);
          int entrySize = input.readInt();
          TimeEvent event = null;
          try {
            event = readSerde.readObject(input, TimeEvent.class);
          } catch(RuntimeException ex) {
            ex.printStackTrace();
          }
          if(event != null) {
            List<TimeEvent> events = bucketDataPerWindow.get(key);
            if(events == null) {
              bucketDataPerWindow.put(key, Lists.newArrayList(event));
            } else {
              events.add(event);
            }

          }
        }
        input.close();
        stream.close();
      }
      finally {
        fs.close();
      }
    }
    DTFileReader bcktReader = readers.get(bucketKey);
    //HFileReader bcktReader = readers.get(bucketKey);
    TreeMap<byte[], byte[]> storedData = null;
    String readerPath = null;
    if(bcktReader != null) {
      //storedData = bcktReader.readFully();
      readerPath = bcktReader.getPath();
      DTFileReader br = createDTReader(readerPath);
      storedData = br.readFully();
      br.close();
    }
    long start = System.currentTimeMillis();
    storeBucketData(bucketDataPerWindow, storedData, bucketKey, readerPath);
    //mergeBucketId = bucketKey;
    bucketDataPerWindow.clear();

    for(Map.Entry<Long, Long> e : bucketPos.entrySet()) {
      Path dataFilePath = new Path(bucketRoot + PATH_SEPARATOR + "Windows" + PATH_SEPARATOR + e.getKey());
      FileSystem fs = null;
      try {
        fs = FileSystem.newInstance(dataFilePath.toUri(), configuration);
        if (fs.exists(dataFilePath)) {
          logger.debug("start delete Window {} for Bucket {}", e.getKey(), bucketKey);
          fs.delete(dataFilePath, true);
          logger.debug("end delete Window {} for Bucket {}", e.getKey(), bucketKey);
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      } finally {
        try {
          fs.close();
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
      bucketSavePositions[bucketIdx] = null;
  }

/*  public void startMergeService()
  {
    bucketMergeTimer = new Timer();

    bucketMergeTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        long start = System.currentTimeMillis();
        //logger.info("run Merge Service------------------- {} ", dirtyBuckets.size());
        if(dirtyBuckets.size() != 0) {
          Set<Long> keyList ;
          synchronized (dirtyBuckets) {
            keyList = new HashSet<Long>(dirtyBuckets);
            dirtyBuckets.clear();
          }
          BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
          NameableThreadFactory threadFactory = new NameableThreadFactory("BucketFetchFactory");
          threadPoolExecutor = new ThreadPoolExecutor(keyList.size(), keyList.size(), bucketMergeSpanInMillis, TimeUnit.MILLISECONDS, queue, threadFactory);
          List<Future<Boolean>> futures = Lists.newArrayList();
          for(Long key: keyList) {
            futures.add(threadPoolExecutor.submit(new BucketWriteCallable(key)));
          }
          for (Future<Boolean> future : futures) {
            try {
              if(future.get()) {

              }
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            } catch (ExecutionException e) {
              throw new RuntimeException(e);
            }
          }
          mergeBuckets = keyList;
          //logger.info(" End of run Merge Service------------------- {} -> {}", System.currentTimeMillis() - start, mergeBuckets.size());
          threadPoolExecutor.shutdownNow();
        }
      }

    }, bucketMergeSpanInMillis, bucketMergeSpanInMillis);
  }*/

  private void setupConfig(Configuration conf)
  {
    int chunkSize = 1024 * 1024 * 12;

    int inputBufferSize = 256 * 1024 * 12;

    int outputBufferSize = 256 * 1024 * 12;
    conf.set("tfile.io.chunk.size", String.valueOf(chunkSize));
    conf.set("tfile.fs.input.buffer.size", String.valueOf(inputBufferSize));
    conf.set("tfile.fs.output.buffer.size", String.valueOf(outputBufferSize));
  }

  public void storeBucketData(Map<Object, List<TimeEvent>> bucketData, TreeMap<byte[], byte[]> storedData, long bucketKey, String basePath)
  {
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

    //Write the size of data and then data
    //dataStream.writeInt(bucketData.size());
    Kryo kryo = new Kryo();
    for (Map.Entry<Object, List<TimeEvent>> entry : bucketData.entrySet()) {
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
    String merge = "";
    if(basePath != null) {
      if(storedData != null) {
        sortedData.putAll(storedData);
      }
      if(!basePath.endsWith("_MERGE")) {
        merge = "_MERGE";
      }
    }

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

  private HFileReader createHReader(String path)
  {
    Path dataFile = new Path(path);
    FileSystem fs = null;
    try {
      fs = FileSystem.newInstance(dataFile.toUri(), configuration);
      if(!fs.exists(dataFile)) {
        return null;
      }
      //FSDataInputStream fsdis = fs.open(dataFile);
      return new HFileReader(fs.getFileStatus(dataFile).getLen(), configuration, path);
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
      //setupConfig(fs.getConf());
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

    //HFileReader reader = readers.get(bucketKey);;

    DTFileReader reader = readers.get(bucketKey);
    if(reader == null)
      return null;

    try {
      byte[] value = reader.get(keyBytes);
      if(value != null)
      {
        Input lInput = new Input(value);
        Kryo kro = new Kryo();
        return (List<T>)kro.readObject(lInput, ArrayList.class);
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

  private class BucketWriteCallable implements Callable<Boolean>
  {

    final Long bucketKey;
    final Integer bucketIdx;

    BucketWriteCallable(Integer bucketIdx, Long bucketKey)
    {
      this.bucketKey = bucketKey;
      this.bucketIdx = bucketIdx;
    }

    @Override
    public Boolean call() throws IOException
    {
      mergeData(bucketIdx, bucketKey);
      return true;
    }
  }

  protected void saveBucketData(long window)
  {
    Map<Integer, Multimap<Object, T>> dataToStore = Maps.newHashMap();
    for (long key: dirtyBuckets) {
      int bucketIdx = (int) (key % noOfBuckets);
      TimeBucket<T> bucket = buckets[bucketIdx];
      dataToStore.put(bucketIdx, bucket.getEvents());
      bucket.transferEvents();
    }
    try {
      if (!dataToStore.isEmpty()) {
        long start = System.currentTimeMillis();
        logger.debug("start store {}", window);
         storeBucketData(window, dataToStore);
        logger.debug("end store {} num {} took {}", window, System.currentTimeMillis() - start);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    dirtyBuckets.clear();
    //committedWindow = window;
  }

  public void storeBucketData(long window, Map<Integer, Multimap<Object, T>> data) throws IOException
  {
    Path dataFilePath = new Path(bucketRoot + PATH_SEPARATOR + "Windows" + PATH_SEPARATOR + window);
    FileSystem fs = FileSystem.newInstance(dataFilePath.toUri(), configuration);
    FSDataOutputStream dataStream = fs.create(dataFilePath);

    Output output = new Output(dataStream);
    try {
      long offset = 0;
      for (int bucketIdx : data.keySet()) {
        Multimap<Object, T> bucketData = data.get(bucketIdx);

        //Write the size of data and then data
        dataStream.writeInt(bucketData.size());
        for (Map.Entry<Object, T> entry : bucketData.entries()) {
          writeSerde.writeObject(output, entry.getKey());
          int posLength = output.position();
          output.writeInt(0); //temporary place holder
          writeSerde.writeObject(output, entry.getValue());
          int posValue = output.position();
          int valueLength = posValue - posLength - 4;
          output.setPosition(posLength);
          output.writeInt(valueLength);
          output.setPosition(posValue);

        }
        output.flush();
        storeBuckets.add(bucketIdx);
        if (bucketPositions[bucketIdx] == null) {
          bucketPositions[bucketIdx] = Maps.newHashMap();
        }
        bucketPositions[bucketIdx].put(window, offset);
        offset = dataStream.getPos();
      }
    }
    finally {
      output.close();
      dataStream.close();
      fs.close();
    }
  }

  public void checkpointed()
  {
    final Set<Integer> bucketIdxes = new HashSet<Integer>(storeBuckets);
    storeBuckets.clear();
    for(Integer key: bucketIdxes) {
      buckets[key].snappshotEvents();
      bucketSavePositions[key] = bucketPositions[key];
      bucketPositions[key] = null;
    }
      ExecutorService threadExecutor = Executors.newFixedThreadPool(1);

    threadExecutor.submit(new Runnable() {
      @Override
      public void run()
      {
        logger.debug("Thread " + Thread.currentThread().getName() + " start saving data...");
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
        NameableThreadFactory threadFactory = new NameableThreadFactory("BucketFetchFactory");
        threadPoolExecutor = new ThreadPoolExecutor(bucketIdxes.size(), bucketIdxes.size(), bucketMergeSpanInMillis, TimeUnit.MILLISECONDS, queue, threadFactory);
        List<Future<Boolean>> futures = Lists.newArrayList();
        for(Integer key: bucketIdxes) {
          futures.add(threadPoolExecutor.submit(new BucketWriteCallable(key, buckets[key].bucketKey)));
        }
        for (Future<Boolean> future : futures) {
          try {
            if(future.get()) {

            }
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          } catch (ExecutionException e) {
            throw new RuntimeException(e);
          }
        }
        mergeBuckets = bucketIdxes;
        //logger.info(" End of run Merge Service------------------- {} -> {}", System.currentTimeMillis() - start, mergeBuckets.size());
        threadPoolExecutor.shutdownNow();

        logger.debug("Thread " + Thread.currentThread().getName() + " stop saving data...");
      }
    });
  }
}


