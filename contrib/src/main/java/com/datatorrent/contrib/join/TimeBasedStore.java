package com.datatorrent.contrib.join;

import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.lib.bucket.Bucketable;
import com.datatorrent.lib.bucket.Event;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
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
import org.apache.hadoop.fs.FileStatus;
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
  private String bucketRoot;
  private Set<Long> mergeBuckets;
  private Map<Integer, Long> bucketWid = new HashMap<Integer, Long>();

  protected transient List<Bucket> expiredBuckets;
  private final transient Lock lock;
  private transient Timer bucketSlidingTimer;
  private transient Timer bucketMergeTimer;
  private transient long bucketMergeSpanInMillis = 10000;
  private Map<Object, Set<Long>> key2Buckets = new HashMap<Object, Set<Long>>();
  private transient Kryo writeSerde;
  protected transient Set<Long> dirtyBuckets;
  static transient final String PATH_SEPARATOR = "/";
  protected transient Configuration configuration;
  protected transient Map<Long, DTFileReader> readers = new HashMap<Long, DTFileReader>();
  protected transient ThreadPoolExecutor threadPoolExecutor;
  private transient long currentWID;
  private transient long mergeWID;
  private transient Set<Long> checkPointedBuckets;
  private transient String TMP_FILE = "._COPYING_";

  public void setBucketRoot(String bucketRoot)
  {
    this.bucketRoot = bucketRoot;
  }

  public TimeBasedStore()
  {
    lock = new Lock();
  }

  public void beginWindow(long window) {
    currentWID = window;
  }

  private void recomputeNumBuckets()
  {
    Calendar calendar = Calendar.getInstance();
    long now = calendar.getTimeInMillis();
    startOfBucketsInMillis = now - spanTimeInMillis;
    expiryTimeInMillis = startOfBucketsInMillis;
    endOBucketsInMillis = now;
    noOfBuckets = (int) Math.ceil((now - startOfBucketsInMillis) / (bucketSpanInMillis * 1.0));
    buckets = (Bucket<T>[]) Array.newInstance(Bucket.class, noOfBuckets);
  }

  public void setup()
  {
    configuration = new Configuration();
    dirtyBuckets = new HashSet<Long>();
    checkPointedBuckets = new HashSet<Long>();
    expiredBuckets = new ArrayList<Bucket>();
    //key2Buckets = new HashMap<Object, Set<Long>>();
    if(buckets == null) {
      recomputeNumBuckets();
    } else {
      recoveryBuckets();
    }

    //ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    this.writeSerde = new Kryo();
    this.writeSerde.register(ArrayList.class, new CollectionSerializer());
    //writeSerde.setClassLoader(classLoader);
    startMergeService();
    startService();

  }

  private void recoveryBuckets()
  {
    long diffFromStart = expiryTimeInMillis - startOfBucketsInMillis;
    long startkey = diffFromStart / bucketSpanInMillis;
    Kryo kryo = new Kryo();
    for(Map.Entry<Integer, Long> bck : bucketWid.entrySet()) {
      Bucket t = buckets[bck.getKey()];
      if(t == null || t.bucketKey < startkey) {
        continue;
      }
      String path = new String(bucketRoot + PATH_SEPARATOR + t.bucketKey + PATH_SEPARATOR + bck.getValue());
      DTFileReader tr = createDTReader(path);
      if(tr != null) {
        readers.put(t.bucketKey, tr);
        /*try {
          List<byte[]> keys = tr.getKeys();
          for(byte[] b : keys) {
            Input lInput = new Input(b);
            Object key = kryo.readObject(lInput, Object.class);
            Set<Long> bucks = key2Buckets.get(key);
            if(bucks == null) {
              bucks = new HashSet<Long>();
              bucks.add(t.bucketKey);
              key2Buckets.put(key, bucks);
            } else {
              bucks.add(t.bucketKey);
            }
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }*/
      }
    }
  }

  public Object getValidTuples(T tuple)
  {
    Kryo kryo = new Kryo();
    Object key = tuple.getEventKey();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output output2 = new Output(bos);
    kryo.writeObject(output2, key);
    output2.close();
    byte[] keyBytes = bos.toByteArray();
    List<Event> validTuples = new ArrayList<Event>();

    Set<Long> keyBuckets = key2Buckets.get(key);
    if(keyBuckets == null)
      return null;
    for(Long bucketKey : keyBuckets) {
      int bucketIdx = (int) (bucketKey % noOfBuckets);
      Bucket tb = buckets[bucketIdx];
      if(tb == null || tb.bucketKey != bucketKey) {
        continue;
      }
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


    /*for(int i = 0; i < noOfBuckets; i++) {
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
    }*/
    return validTuples;
  }

  public Boolean put(T tuple)
  {
    long bucketKey = getBucketKeyFor(tuple);
    if(bucketKey < 0) {
      return false;
    }
    newEvent(bucketKey, tuple);
    return true;
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
        expiredBuckets.add(bucket);
      }
      bucket = createBucket(bucketKey);
      buckets[bucketIdx] = bucket;
    }
    bucket.addNewEvent(bucket.getEventKey(event), event);
    Object key = bucket.getEventKey(event);
    Set<Long> keyBcks =  key2Buckets.get(key);
    if(keyBcks == null) {
      keyBcks = new HashSet<Long>();
      keyBcks.add(bucketKey);
      key2Buckets.put(key, keyBcks);
    } else {
      keyBcks.add(bucketKey);
    }
    dirtyBuckets.add(bucketKey);
  }

  public void startService()
  {
    bucketSlidingTimer = new Timer();
    endOBucketsInMillis = expiryTimeInMillis + (noOfBuckets * bucketSpanInMillis);
    logger.info("bucket properties {}, {}", spanTimeInMillis, bucketSpanInMillis);
    logger.info("bucket time params: start {}, end {}", startOfBucketsInMillis, endOBucketsInMillis);

    bucketSlidingTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        long time = 0;

        synchronized (lock) {
          time = (expiryTimeInMillis += bucketSpanInMillis);
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
    Iterator<Bucket> exIterator = expiredBuckets.iterator();
    for (; exIterator.hasNext(); ) {
      Bucket t = exIterator.next();
      if(t == null) {
        continue;
      }
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

  public void endWindow()
  {
    if(mergeBuckets != null && mergeBuckets.size() != 0) {
      checkPointedBuckets.addAll(mergeBuckets);
      logger.info("End Window:  {} -> {}", bucketRoot, mergeBuckets.size());
      for(long mergeBucketId: mergeBuckets) {
        int bckIdx = (int) (mergeBucketId % noOfBuckets);
        Bucket bucket = buckets[bckIdx];
        bucket.transferDataFromMemoryToStore();

        DTFileReader bcktReader = readers.get(mergeBucketId);
        String readerPath = null;
        if(bcktReader != null) {
          readerPath = bcktReader.getPath();
          try {
            bcktReader.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        String path = new String(bucketRoot + PATH_SEPARATOR + ((Bucket) bucket).bucketKey + PATH_SEPARATOR + mergeWID);
        DTFileReader tr = createDTReader(path);
        if (tr != null) {
          logger.info("Map Changed: {} -> {}", bucketRoot, path);
          readers.put(bucket.bucketKey, tr);
          bucketWid.put(bckIdx, mergeWID);
        }
      }
      mergeBuckets = null;
    }
}

  protected void saveData(long bucketKey, long windowId) throws IOException
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
    logger.info("Save - 1 : {} -> {}", bucketRoot, windowId);
    if(bcktReader != null) {
      readerPath = bcktReader.getPath();
      logger.info("Save - 2 : {} -> {}", bucketRoot, windowId);
      DTFileReader br = createDTReader(readerPath);
      logger.info("Save - 3 : {} -> {}", bucketRoot, windowId);
      if(br == null) {
        logger.info("Empty File------: {} -> {}", bucketRoot, readerPath);
      }
      storedData = br.readFully();
      logger.info("Save - 4 : {} -> {} -> {}", bucketRoot, windowId, storedData.size());
      br.close();
    }
    logger.info("Save - 5 : {} -> {}", bucketRoot, windowId);
    storeBucketData(events, storedData, bucketKey, windowId);
    events.clear();
    logger.info("Save - 6 : {} -> {}", bucketRoot, windowId);
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
        Long windowId = currentWID;
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
            futures.add(threadPoolExecutor.submit(new BucketWriteCallable(key, windowId)));
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
          mergeWID = windowId;
          logger.info(" End of run Merge Service------------------- {} -> {}", System.currentTimeMillis() - start, mergeBuckets.size());
          threadPoolExecutor.shutdownNow();
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

  public void storeBucketData(Map<Object, List<T>> bucketData, TreeMap<byte[], byte[]> storedData, long bucketKey, long windoId)
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
      List<T> entriesList = entry.getValue();
      if(storedData != null) {
        byte[] valueBytes = storedData.remove(bos.toByteArray());
        if(valueBytes != null) {
          Input lInput = new Input(valueBytes);
          Kryo kryo = new Kryo();
          entriesList.addAll((List<T>)kryo.readObject(lInput, ArrayList.class));
        }
      }
      ByteArrayOutputStream bos1 = new ByteArrayOutputStream();
      Output output1 = new Output(bos1);
      this.writeSerde.writeObject(output1, entriesList);
      output1.close();
      sortedData.put(bos.toByteArray(), bos1.toByteArray());
    }

    if(storedData != null)
      sortedData.putAll(storedData);
    Path dataFilePath = new Path(bucketRoot + PATH_SEPARATOR + bucketKey + PATH_SEPARATOR + windoId);
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
        Kryo kryo = new Kryo();
        return (List<T>)kryo.readObject(lInput, ArrayList.class);
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
    return null;
  }

  public void setSpanTimeInMillis(long spanTimeInMillis)
  {
    this.spanTimeInMillis = spanTimeInMillis;
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

  public void checkpointed(long windowId)
  {
    for(Long key: checkPointedBuckets) {
      logger.info("CheckPointed - 1 : {} -> {} -> {}", bucketRoot, mergeWID, windowId);
      String bucketPath = bucketRoot + PATH_SEPARATOR + key + PATH_SEPARATOR;
      String bWid = readers.get(key).getPath().substring(bucketPath.length());
      Long maxWId = Long.parseLong(bWid);
      Path dataFile = new Path(bucketPath);
      FileSystem fs = null;
      try {
        fs = FileSystem.newInstance(dataFile.toUri(), configuration);
        if (fs.exists(dataFile)) {
          FileStatus[] fileStatuses = fs.listStatus(dataFile);

          for (FileStatus operatorDirStatus : fileStatuses) {
            String fileName = operatorDirStatus.getPath().getName();
            if(fileName.endsWith(TMP_FILE)) {
              continue;
            }
            long wId = Long.parseLong(fileName);
            //long wId = Long.parseLong(fileName, 16);
            if(wId < maxWId) {
              Path file = new Path(bucketRoot + PATH_SEPARATOR + key + PATH_SEPARATOR + wId);
              FileSystem fsSys = FileSystem.newInstance(file.toUri(), configuration);
              if (fs.exists(file)) {
                logger.info("start delete {} -> {}", key, wId);
                fs.delete(file, true);
                logger.info("end delete {} -> {}", key, wId);
              }
            }
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    checkPointedBuckets.clear();
  }

  private class BucketWriteCallable implements Callable<Boolean>
  {

    final long bucketKey;
    final long windowId;

    BucketWriteCallable(long bucketKey, long windowId)
    {
      this.bucketKey = bucketKey;
      this.windowId = windowId;
    }

    @Override
    public Boolean call() throws IOException
    {
      saveData(bucketKey, windowId);
      return true;
    }
  }
}
