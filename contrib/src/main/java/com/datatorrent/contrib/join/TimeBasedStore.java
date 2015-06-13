package com.datatorrent.contrib.join;

import com.datatorrent.common.util.NameableThreadFactory;
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
  protected Bucket<T>[] buckets;
  protected long expiryTimeInMillis;
  protected long spanTimeInMillis;
  protected int bucketSpanInMillis = 30000;
  protected long startOfBucketsInMillis;
  protected long endOBucketsInMillis;
  private final transient Lock lock;
  private long expiryTime;
  private String bucketRoot;
  private Set<Long> mergeBuckets;
  protected Map<Long, Bucket> expiredBuckets;
  private boolean isOuter=false;

  private transient Timer bucketSlidingTimer;
  private transient Timer bucketMergeTimer;
  private transient long bucketMergeSpanInMillis = 23000;
  private transient Kryo writeSerde;
  protected transient Set<Long> dirtyBuckets;
  static transient final String PATH_SEPARATOR = "/";
  protected transient Configuration configuration;
  protected transient Map<Long, DTFileReader> readers = new HashMap<Long, DTFileReader>();
  protected transient ThreadPoolExecutor threadPoolExecutor;
  private transient List<T> unmatchedEvents = new ArrayList<T>();

  public void setBucketRoot(String bucketRoot)
  {
    this.bucketRoot = bucketRoot;
  }

  public TimeBasedStore()
  {
    lock = new Lock();
    expiredBuckets = new HashMap<Long, Bucket>();
  }

  private void recomputeNumBuckets()
  {
    Calendar calendar = Calendar.getInstance();
    long now = calendar.getTimeInMillis();
    startOfBucketsInMillis = now - spanTimeInMillis;
    expiryTime = startOfBucketsInMillis;
    expiryTimeInMillis = startOfBucketsInMillis;
    endOBucketsInMillis = now;
    noOfBuckets = (int) Math.ceil((now - startOfBucketsInMillis) / (bucketSpanInMillis * 1.0));
    buckets = (Bucket<T>[]) Array.newInstance(Bucket.class, noOfBuckets);
  }

  public void setup()
  {
    configuration = new Configuration();
    recomputeNumBuckets();
    //ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    this.writeSerde = new Kryo();
    //writeSerde.setClassLoader(classLoader);
    startMergeService();
    startService();
    dirtyBuckets = new HashSet<Long>();
  }

  public void setExpiryTime(long expiryTime)
  {
    this.expiryTime = expiryTime;
  }

  public Object getValidTuples(T tuple)
  {
    Object key = tuple.getEventKey();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output output2 = new Output(bos);
    this.writeSerde.writeObject(output2, key);
    output2.close();
    byte[] keyBytes = bos.toByteArray();
    List<Event> validTuples = new ArrayList<Event>();

    for(int i = 0; i < noOfBuckets; i++) {
      Bucket tb = buckets[i];
      if(tb == null) {
        continue;
      }
      if(tb.contains(key)) {
        List<T> events = tb.get(key);
        if (events != null) {
          validTuples.addAll(events);
        }
        if (tb.isDataOnDiskLoaded()) {
          List<T> dataEvents = getDataFromFile(keyBytes, tb.bucketKey);
          if (dataEvents != null) {
            validTuples.addAll(dataEvents);
          }
        }
      }
    }
    return validTuples;
  }

  public void put(T tuple)
  {
    long bucketKey = getBucketKeyFor(tuple);
    newEvent(bucketKey, tuple);
  }

  public long getBucketKeyFor(T event)
  {
    long eventTime = event.getTime();
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

  private static class Lock
  {
  }

  public void newEvent(long bucketKey, T event)
  {
    int bucketIdx = (int) (bucketKey % noOfBuckets);

    Bucket<T> bucket = buckets[bucketIdx];

    if (bucket == null || bucket.bucketKey != bucketKey) {
      if (bucket != null) {
        expiredBuckets.put(bucket.bucketKey, bucket);
      }
      bucket = createBucket(bucketKey);
      buckets[bucketIdx] = bucket;
    }
    bucket.addNewEvent(bucket.getEventKey(event), event);
    dirtyBuckets.add(bucketKey);
  }

  public void startService()
  {
    bucketSlidingTimer = new Timer();
    endOBucketsInMillis = expiryTime + (noOfBuckets * bucketSpanInMillis);
    logger.debug("bucket properties {}, {}", spanTimeInMillis, bucketSpanInMillis);
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
      Bucket t = (Bucket) expiredBuckets.get(key);
      logger.info("DeleteExpiredB - 1: File: {}, time: {}", bucketRoot, time);
      if (startOfBucketsInMillis + (t.bucketKey * noOfBuckets) < time) {
        logger.info("DeleteExpiredB - 2: File: {}, time: {},  key: {}", bucketRoot, time, t.bucketKey);
        deleteBucket(t);
        exIterator.remove();
      }
    }
  }

  private void deleteBucket(Bucket bucket)
  {
    if (bucket == null) {
      return;
    }
    String bucketPath = null;
    try {

      DTFileReader bcktReader = readers.remove(bucket.bucketKey);
      if(isOuter) {
        // Check the events which are unmatched and add those into the unmatchedEvents list
        TreeMap<byte[] , byte[]> data = bcktReader.readFully();

        for(Map.Entry<byte[], byte[]> e: data.entrySet()) {
          Input lInput = new Input(e.getValue());
          List<T> eventList = (List<T>)this.writeSerde.readObject(lInput, ArrayList.class);
          for (T event : eventList) {
            if (!((TimeEvent) (event)).isMatch()) {
              unmatchedEvents.add(event);
            }
          }
        }
      }

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
  }

  public void endWindow(long window)
  {
    if(mergeBuckets != null && mergeBuckets.size() != 0) {
      for(long mergeBucketId: mergeBuckets) {
        Bucket bucket = buckets[((int) (mergeBucketId % noOfBuckets))];
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
        String path = new String(bucketRoot + PATH_SEPARATOR + ((Bucket) bucket).bucketKey + merge);
        DTFileReader tr = createDTReader(path);
        if (tr != null) {
          readers.put(((Bucket) bucket).bucketKey, tr);
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

  protected void saveData(long bucketKey) throws IOException
  {
    int bucketIdx = (int) (bucketKey % noOfBuckets);
    Bucket<T> bucket = buckets[bucketIdx];
    if(bucket == null || bucket.getEvents() == null || bucket.getEvents().isEmpty()) {
      return;
    }
    bucket.transferEvents();

    Map<Object, List<T>> events = new HashMap<Object, List<T>>(bucket.getWrittenEvents());
    DTFileReader bcktReader = readers.get(bucketKey);
    TreeMap<byte[], byte[]> storedData = null;
    String readerPath = null;
    if(bcktReader != null) {
      readerPath = bcktReader.getPath();
      DTFileReader br = createDTReader(readerPath);
      storedData = br.readFully();
      br.close();
    }
    storeBucketData(events, storedData, bucketKey, readerPath);
    events.clear();
  }

  public void startMergeService()
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
  }
  private void setupConfig(Configuration conf)
  {
    int chunkSize = 1024 * 1024 * 12;

    int inputBufferSize = 256 * 1024 * 12;

    int outputBufferSize = 256 * 1024 * 12;
    conf.set("tfile.io.chunk.size", String.valueOf(chunkSize));
    conf.set("tfile.fs.input.buffer.size", String.valueOf(inputBufferSize));
    conf.set("tfile.fs.output.buffer.size", String.valueOf(outputBufferSize));
  }

  public void storeBucketData(Map<Object, List<T>> bucketData, TreeMap<byte[], byte[]> storedData, long bucketKey, String basePath)
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
    for (Map.Entry<Object, List<T>> entry : bucketData.entrySet()) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Output output2 = new Output(bos);
      this.writeSerde.writeObject(output2, entry.getKey());
      output2.close();
      ByteArrayOutputStream bos1 = new ByteArrayOutputStream();
      Output output1 = new Output(bos1);
      this.writeSerde.writeObject(output1, entry.getValue());
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
    DTFileReader reader = readers.get(bucketKey);
    if(reader == null)
      return null;

    try {
      byte[] value = reader.get(keyBytes);
      if(value != null)
      {
        Input lInput = new Input(value);
        return (List<T>)this.writeSerde.readObject(lInput, ArrayList.class);
      }
    } catch (IOException e) {
      throw new RuntimeException("Excetpion from " + reader.getPath() + " ==>  " + e);
    }
    return null;
  }

  protected Bucket<T> createBucket(long bucketKey)
  {
    return new Bucket<T>(bucketKey);
  }

  /**
   * Return the unmatched events which are present in the expired buckets
   * @return
   */
  public List<T> getUnmatchedEvents()
  {
    List<T> copyEvents = new ArrayList<T>(unmatchedEvents);
    unmatchedEvents.clear();
    return copyEvents;
  }

  public void setSpanTimeInMillis(long spanTimeInMillis)
  {
    this.spanTimeInMillis = spanTimeInMillis;
  }

  public void setBucketSpanInMillis(int bucketSpanInMillis)
  {
    this.bucketSpanInMillis = bucketSpanInMillis;
  }

  public void isOuterJoin(boolean isOuter)
  {
    this.isOuter = isOuter;
  }

  public void shutdown()
  {
    bucketSlidingTimer.cancel();
    bucketMergeTimer.cancel();
  }

  private class BucketWriteCallable implements Callable<Boolean>
  {

    final long bucketKey;

    BucketWriteCallable(long bucketKey)
    {
      this.bucketKey = bucketKey;
    }

    @Override
    public Boolean call() throws IOException
    {
      saveData(bucketKey);
      return true;
    }
  }
}
