package com.datatorrent.contrib.join;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.file.tfile.TFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.common.util.Pair;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.google.common.collect.Lists;
import javax.validation.constraints.Min;

public class TimeBasedStore<T extends TimeEvent>
{
  private static transient final Logger logger = LoggerFactory.getLogger(TimeBasedStore.class);
  @Min(1)
  protected int noOfBuckets;
  protected transient Bucket[] buckets;
  protected long expiryTimeInMillis;
  protected long spanTimeInMillis;
  protected int bucketSpanInMillis = 30000;
  protected long startOfBucketsInMillis;
  protected long endOBucketsInMillis;
  private String bucketRoot;
  private transient Set<Long> mergeBuckets;
  private transient Map<Integer, Long> bucketWid = new HashMap<Integer, Long>();

  protected transient List<Bucket> expiredBuckets;
  private final transient Lock lock;
  private transient Timer bucketSlidingTimer;
  private transient Timer bucketMergeTimer;
  private transient long bucketMergeSpanInMillis = 10000;
  //private Map<Object, Set<Long>> key2Buckets = new HashMap<Object, Set<Long>>();
  private transient Kryo writeSerde;
  protected transient Set<Long> dirtyBuckets;
  static transient final String PATH_SEPARATOR = "/";
  protected transient Configuration configuration;
  protected transient Map<Long, BucketReader> readers = new HashMap<Long, BucketReader>();
  protected transient Map<Long, BucketReader> updatedReaders = new HashMap<Long, BucketReader>();
  protected transient ThreadPoolExecutor threadPoolExecutor;
  private transient long currentWID;
  private transient long mergeWID;
  private transient String TMP_FILE = "._COPYING_";
  private transient Executor writeExecutor;
  private long flushSize = 1;
  private long maxFileDataSizeInBytes = 1024 * 1024 * 70;

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
    buckets = (Bucket[]) Array.newInstance(Bucket.class, noOfBuckets);
  }

  public void setup()
  {
    configuration = new Configuration();
    dirtyBuckets = new HashSet<Long>();
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
    //startMergeService();
    startService();
    writeExecutor = Executors.newSingleThreadScheduledExecutor(new NameableThreadFactory(this.getClass().getSimpleName() + "-Writer"));
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
        //readers.put(t.bucketKey, tr);
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

  public Object getValidTuples(byte[] tuple, byte[] keyBytes)
  {
    List<byte[]> validTuples = new ArrayList<byte[]>();

    /*Set<Long> keyBuckets = key2Buckets.get(key);
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
    }*/


    for(int i = 0; i < noOfBuckets; i++) {
      Bucket tb = buckets[i];
      if(tb == null) {
        continue;
      }
      if(tb.contains(keyBytes)) {
        List<byte[]> events = tb.get(keyBytes);
        if (events != null) {
          validTuples.addAll(events);
        }
        if (tb.isDataOnDiskLoaded()) {
          List<byte[]> dataEvents = getDataFromFile(keyBytes, tb.bucketKey);
          if (dataEvents != null) {
            validTuples.addAll(dataEvents);
          }
        }
      }
    }
    return validTuples;
  }

  public Boolean put(byte[] tuple, long time, byte[] keyBytes)
  {
    long bucketKey = getBucketKeyFor(time);
    if(bucketKey < 0) {
      return false;
    }
    newEvent(bucketKey, tuple, keyBytes);
    return true;
  }

  public long getBucketKeyFor(long eventTime)
  {
    //long eventTime = event.getTime();
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

  public void newEvent(long bucketKey, byte[] event, byte[] keyBytes)
  {
    int bucketIdx = (int) (bucketKey % noOfBuckets);

    Bucket bucket = buckets[bucketIdx];

    if (bucket == null || bucket.bucketKey != bucketKey) {
      if (bucket != null) {
        expiredBuckets.add(bucket);
      }
      bucket = createBucket(bucketKey);
      buckets[bucketIdx] = bucket;
    }
    bucket.addNewEvent(keyBytes, event);
    //Object key = bucket.getEventKey(event);
    /*Set<Long> keyBcks =  key2Buckets.get(key);
    if(keyBcks == null) {
      keyBcks = new HashSet<Long>();
      keyBcks.add(bucketKey);
      key2Buckets.put(key, keyBcks);
    } else {
      keyBcks.add(bucketKey);
    }*/
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
    Iterator<Bucket> exIterator = expiredBuckets.iterator();
    for (; exIterator.hasNext(); ) {
      Bucket t = exIterator.next();
      if(t == null) {
        continue;
      }
      if (startOfBucketsInMillis + ((t.bucketKey + 1) * bucketSpanInMillis) < time) {
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

    try {
      BucketReader bcktReader = readers.remove(bucket.bucketKey);

      if (bcktReader != null) {
        bcktReader.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    bucket.clear();
    String bucketPath = bucketRoot + PATH_SEPARATOR + bucket.bucketKey + PATH_SEPARATOR;
    Path dataFile = new Path(bucketPath);
    FileSystem fs = null;
    try {
      fs = FileSystem.newInstance(dataFile.toUri(), configuration);
      if (fs.exists(dataFile)) {
        FileStatus[] fileStatuses = fs.listStatus(dataFile);

        for (FileStatus operatorDirStatus : fileStatuses) {
          FileSystem fsSys = null;
          try{
            String fileName = operatorDirStatus.getPath().getName();
            if(fileName.endsWith(TMP_FILE)) {
              continue;
            }
            long wId = Long.parseLong(fileName);
            Path file = new Path(bucketRoot + PATH_SEPARATOR + bucket.bucketKey + PATH_SEPARATOR + wId);
            fsSys = FileSystem.newInstance(file.toUri(), configuration);
            if (fsSys.exists(file)) {
              FileStatus[] file2Statuses = fsSys.listStatus(dataFile);
              for (FileStatus oDirStatus : file2Statuses) {
                FileSystem fs2Sys = null;
                String fName = oDirStatus.getPath().getName();
                Path path = new Path(bucketRoot + PATH_SEPARATOR + bucket.bucketKey + PATH_SEPARATOR + wId);
                fs2Sys = FileSystem.newInstance(path.toUri(), configuration);
                if(fs2Sys.exists(path)) {
                  fs2Sys.delete(path, true);
                }
              }
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          finally {
            try {
              fs.close();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    finally {
      try {
        fs.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void endWindow()
  {
    for(Long bKey : dirtyBuckets) {
      int bckIdx = (int) (bKey % noOfBuckets);
      Bucket bucket = buckets[bckIdx];
      if(bucket == null) {
        continue;
      }
      /*try {
        bucket.wal.endWindow(currentWID);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }*/
    }

    if(mergeBuckets != null && mergeBuckets.size() != 0 && mergeBuckets.size() == updatedReaders.size()) {

      for(long mergeBucketId: mergeBuckets) {
        int bckIdx = (int) (mergeBucketId % noOfBuckets);
        Bucket bucket = buckets[bckIdx];
        if(bucket.bucketKey != mergeBucketId)
          continue;
        bucket.transferDataFromMemoryToStore();
      }
      readers.putAll(updatedReaders);
      updatedReaders.clear();
      mergeBuckets = null;
    }
}

  protected void saveData(long bucketKey, long windowId) throws IOException
  {
    int bucketIdx = (int) (bucketKey % noOfBuckets);
    Bucket bucket = buckets[bucketIdx];
    if(bucket == null /*|| bucket.getEvents() == null || bucket.getEvents().isEmpty()*/) {
      return;
    }
    //bucket.transferEvents();

    Map<ByteBuffer, List<byte[]>> events = new HashMap<ByteBuffer, List<byte[]>>(bucket.getWrittenEvents());
    BucketReader bcktReader = readers.get(bucketKey);
    TreeMap<byte[], byte[]> storedData = null;
    String readerPath = null;
    if(bcktReader != null) {
      for(Map.Entry<byte[], DTFileReader> fr: bcktReader.files.entrySet()) {
        readerPath = fr.getValue().getPath();
        DTFileReader br = createDTReader(readerPath);
        if(storedData == null) {
          storedData = new TreeMap<byte[], byte[]>(new DefaultKeyComparator());
        }
        storedData.putAll(br.readFully());
        br.close();
      }
    }
    storeBucketData(events, storedData, bucketKey, windowId);
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
          logger.info(" End of run Merge Service- {} -> {}", System.currentTimeMillis() - start, mergeBuckets.size());
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

  public static class DefaultKeyComparator implements Comparator<byte[]>
  {
    @Override
    public int compare(byte[] o1, byte[] o2)
    {
      return WritableComparator.compareBytes(o1, 0, o1.length, o2, 0, o2.length);
    }
  }

  public void storeBucketData(Map<ByteBuffer, List<byte[]>> bucketData, TreeMap<byte[], byte[]> storedData, long bucketKey, long windoId)
  {

    TreeMap<byte[], byte[]> stData = new TreeMap<byte[], byte[]>();
    TreeMap<byte[], byte[]> sortedData = new TreeMap<byte[], byte[]>(new DefaultKeyComparator());
    //Write the size of data and then data
    //dataStream.writeInt(bucketData.size());
    Kryo kryo = new Kryo();
    for (Map.Entry<ByteBuffer, List<byte[]>> entry : bucketData.entrySet()) {
      List<byte[]> entriesList = entry.getValue();
      if(storedData != null) {
        byte[] valueBytes = storedData.remove(entry.getKey().array());
        if(valueBytes != null) {
          Input lInput = new Input(valueBytes);
          entriesList.addAll((List<byte[]>)kryo.readObject(lInput, ArrayList.class));
        }
      }
      ByteArrayOutputStream bos1 = new ByteArrayOutputStream();
      Output output1 = new Output(bos1);
      kryo.writeObject(output1, entriesList);
      output1.close();
      sortedData.put(entry.getKey().array(), bos1.toByteArray());
    }
    if(storedData != null)
      sortedData.putAll(storedData);

    int fileSeq = 0;
    String path = bucketRoot + PATH_SEPARATOR + bucketKey + PATH_SEPARATOR + windoId + PATH_SEPARATOR + "TMP_" + fileSeq;
    Path dataFilePath = new Path(path);
    FSDataOutputStream dataStream = null;
    FileSystem fs = null;
    TFile.Writer writer = null;
    List<Pair<byte[], Integer>> fileStartKeys = new ArrayList<Pair<byte[], Integer>>();
    try {
      int minBlockSize = 64 * 1024;

      String compressName = TFile.COMPRESSION_NONE;

      String comparator = "memcmp";

      for(Map.Entry<byte[], byte[]> entry : sortedData.entrySet()) {
        if(writer == null) {
          path = bucketRoot + PATH_SEPARATOR + bucketKey + PATH_SEPARATOR + windoId + PATH_SEPARATOR + "TMP_" + fileSeq;
          dataFilePath = new Path(path);
          fs = FileSystem.newInstance(dataFilePath.toUri(), configuration);
          dataStream = fs.create(dataFilePath);
          setupConfig(fs.getConf());
          writer  = new TFile.Writer(dataStream, minBlockSize, compressName, comparator, configuration);
          fileStartKeys.add(new Pair<byte[],Integer>(entry.getKey(), fileSeq));
        }
        writer.append(entry.getKey(), entry.getValue());
        if(dataStream.size() > maxFileDataSizeInBytes) {
          fileSeq++;
          writer.close();
          dataStream.close();
          fs.close();
          writer = null;
        }
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
    BucketReader br = new BucketReader(new DefaultKeyComparator());
    for(Pair<byte[], Integer> e : fileStartKeys) {
      path = bucketRoot + PATH_SEPARATOR + bucketKey + PATH_SEPARATOR + windoId + PATH_SEPARATOR + "TMP_" +  e.getSecond();
      DTFileReader f = createDTReader(path);
      if(f != null)
        br.files.put(e.getFirst(), f);
    }
    updatedReaders.put(bucketKey, br);
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

  private List<byte[]> getDataFromFile(byte[] keyBytes, long bucketKey)
  {
    Map.Entry<byte[], DTFileReader> readerEntry = readers.get(bucketKey).files.floorEntry(keyBytes);
    if(readerEntry == null)
      return null;

    try {
      byte[] value = readerEntry.getValue().get(keyBytes);
      if(value != null)
      {
        Input lInput = new Input(value);
        Kryo kryo = new Kryo();
        return (List<byte[]>)kryo.readObject(lInput, ArrayList.class);
      }
    } catch (IOException e) {
      throw new RuntimeException("Excetpion from " + readerEntry.getValue().getPath() + " ==>  " + e);
    }
    return null;
  }

  protected Bucket createBucket(long bucketKey)
  {
    Bucket b = new Bucket(bucketKey);
    return b;
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

  public void checkpointed(final long windowId)
  {

  }

  public void committed(long windowId)
  {
    final long commitedWId = windowId;
    if(mergeBuckets != null) {
      return;
    }
    for(final Long bcktKey : dirtyBuckets) {
      int idx = (int) (bcktKey % noOfBuckets);
      Bucket b = buckets[idx];
      if(b == null || b.bucketKey != bcktKey) {
        continue;
      }
      if(b.getEvents() != null && b.getEvents().size() < this.flushSize) {
        continue;
      }
      if(mergeBuckets == null) {
        mergeBuckets = new HashSet<Long>();
      }
      mergeBuckets.add(bcktKey);
      b.transferEvents();
      Runnable flushRunnable = new Runnable() {
        @Override
        public void run()
        {
          try {
            //writeDataFiles(bucket);
            saveData(bcktKey, commitedWId);
          } catch (Throwable e) {
            throw new RuntimeException("Write error: " + e.getMessage());
          }
        }
      };
      this.writeExecutor.execute(flushRunnable);
    }
    if(mergeBuckets != null)
      dirtyBuckets.removeAll(mergeBuckets);
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

  public long getFlushSize()
  {
    return flushSize;
  }

  public void setFlushSize(long flushSize)
  {
    this.flushSize = flushSize;
  }

  public long getMaxFileDataSizeInBytes()
  {
    return maxFileDataSizeInBytes;
  }

  public void setMaxFileDataSizeInBytes(long maxFileDataSizeInBytes)
  {
    this.maxFileDataSizeInBytes = maxFileDataSizeInBytes;
  }
}
