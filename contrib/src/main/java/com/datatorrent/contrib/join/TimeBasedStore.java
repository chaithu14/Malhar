package com.datatorrent.contrib.join;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.Pair;
import com.datatorrent.lib.bucket.Bucketable;
import com.datatorrent.lib.bucket.Event;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.DTFile;
import org.apache.hadoop.io.file.tfile.TFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeBasedStore<T extends Event & Bucketable>
{
  private static transient final Logger logger = LoggerFactory.getLogger(TimeBasedStore.class);
  @Min(1)
  protected int noOfBuckets;
  protected transient TimeBucket<T>[] buckets;
  protected long expiryTimeInMillis = 1 ;
  protected long spanTime = 1 ;
  protected int bucketSpanInMillis = 30000;
  protected long startOfBucketsInMillis;
  protected long endOBucketsInMillis;
  private final transient Lock lock;
  private Map<Object, List<Long>> key2Buckets;
  private transient Timer bucketSlidingTimer;
  private long expiryTime;
  private Long[] maxTimesPerBuckets;
  private String bucketRoot;
  private transient Kryo writeSerde;
  protected Map<Long, Long>[] bucketPositions;
  protected Map<Long, Long> windowToTimestamp;
  protected transient Multimap<Long, Integer> windowToBuckets;
  @NotNull
  protected final Map<Integer, TimeBucket<T>> dirtyBuckets;
  static transient final String PATH_SEPARATOR = "/";
  protected transient Configuration configuration;


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
    dirtyBuckets = new HashMap<Integer, TimeBucket<T>>();
    windowToTimestamp = Maps.newHashMap();
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
    maxTimesPerBuckets = new Long[noOfBuckets];
  }

  public void setup(Context.OperatorContext context)
  {
    configuration = new Configuration();
    recomputeNumBuckets();
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    this.writeSerde = new Kryo();
    writeSerde.setClassLoader(classLoader);

    bucketPositions = (Map<Long, Long>[]) Array.newInstance(HashMap.class, noOfBuckets);
    windowToBuckets = ArrayListMultimap.create();
    for (int i = 0; i < bucketPositions.length; i++) {
      if (bucketPositions[i] != null) {
        for (Long window : bucketPositions[i].keySet()) {
          windowToBuckets.put(window, i);
        }
      }
    }

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
    List<Long> keyBuckets = key2Buckets.get(key);
    if(keyBuckets == null) {
      //logger.info("GetValidTuples - 1: " + tuple);
      return null;
    }
    long start = System.currentTimeMillis();
    List<Event> validTuples = new ArrayList<Event>();
    //logger.info("GetValidTuples - 2: " + tuple);
    for(Long idx: keyBuckets) {
      //logger.info("GetValidTuples - 3: " + tuple);
      if(expiredBuckets.get(idx) != null) {
        //logger.info("GetValidTuples - 4: " + tuple);
        TimeBucket tb = (TimeBucket)expiredBuckets.get(idx);
        List<T> events = tb.get(key);
        if(events != null) {
          //logger.info("GetValidTuples - 5: " + tuple);
          for(T event: events) {
            if(Math.abs(tuple.getTime() - event.getTime()) < spanTime) {
              validTuples.add(event);
            }
          }
        }
      } else {
        //logger.info("GetValidTuples - 8: " + tuple);
        int bucketIdx = (int) (idx % noOfBuckets);
        TimeBucket tb = (TimeBucket)buckets[bucketIdx];
        if(tb == null) {
          //logger.info("GetValidTuples - 9: " + tuple);
          continue;
        }
        List<T> events = tb.get(key);
        if(tb.isDataOnDiskLoaded()) {
          Map<Long, Long> windows = Maps.newHashMap(bucketPositions[bucketIdx]);
          for(Map.Entry<Long, Long> window: windows.entrySet()) {
            //logger.info("windooooooooooo: {}", window.getKey());
            try {
              List<T> dataEvents = getDataFromFile(window.getKey(), keyBytes, tb.bucketKey);
              if(dataEvents != null) {
                validTuples.addAll(dataEvents);
              }
              //logger.info("windooooooooooo: {}", dataEvents);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
        if(events != null) {
          validTuples.addAll(events);
        }
      }
    }
    logger.info("Time took for Valid Tuples: {}", System.currentTimeMillis() - start);
    return validTuples;
  }

  private void updateBuckets(long time) {
    if(time < endOBucketsInMillis) {
      return;
    }
    int count =(int) ((time - endOBucketsInMillis)/(bucketSpanInMillis * 1.0));
    for(int i = 0; i < count ; i++) {
      TimeBucket<T> b = buckets[i];
      if(b != null) {
        //ogger.info("updateBuckets:    --- {} -> {}", b.bucketKey, System.currentTimeMillis());
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
    return  event.getTime();
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
      if(bucket != null) {
        //logger.info("NewEvent:    --- {}", bucket.bucketKey);
        expiredBuckets.put(bucket.bucketKey, bucket);
      }
      bucket = createBucket(bucketKey);
      buckets[bucketIdx] = bucket;
      dirtyBuckets.put(bucketIdx, bucket);
    } else if (dirtyBuckets.get(bucketIdx) == null) {
      dirtyBuckets.put(bucketIdx, bucket);
    }

    Object key = bucket.getEventKey(event);
    List<Long> keyBuckets = key2Buckets.get(key);
    if(keyBuckets == null) {
      key2Buckets.put(key, Lists.newArrayList(bucketKey));
    } else {
      key2Buckets.get(key).add(bucketKey);
    }
    bucket.addNewEvent(bucket.getEventKey(event), event);
    //logger.info("Bucket IDx: {}", bucketIdx);
    Long max = maxTimesPerBuckets[bucketIdx];
    long eventTime = getTime(event);
    if (max == null || eventTime > max) {
      maxTimesPerBuckets[bucketIdx] = eventTime;
    }
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
        updateBuckets(endOBucketsInMillis + bucketSpanInMillis);
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

  void deleteExpiredBuckets(long time) throws IOException {
    Iterator<Long> iterator = windowToBuckets.keySet().iterator();
    for (; iterator.hasNext(); ) {
      long window = iterator.next();
      long timestamp= windowToTimestamp.get(window);
      if (timestamp < time) {
        Collection<Integer> indices = windowToBuckets.get(window);
        synchronized (indices) {
          if (indices.size() > 0) {
            Path dataFilePath = new Path(bucketRoot + PATH_SEPARATOR + window);
            FileSystem fs = FileSystem.newInstance(dataFilePath.toUri(), configuration);
            try {
              if (fs.exists(dataFilePath)) {
                logger.debug("start delete {}", window);
                fs.delete(dataFilePath, true);
                logger.debug("end delete {}", window);
              }
              for (int bucketIdx : indices) {
                Map<Long, Long> offsetMap = bucketPositions[bucketIdx];
                if (offsetMap != null) {
                  synchronized (offsetMap) {
                    offsetMap.remove(window);
                  }
                }
              }
            }
            finally {
              fs.close();
            }
          }
          windowToTimestamp.remove(window);
          iterator.remove();
        }
      }
    }
    Iterator<Long> exIterator = expiredBuckets.keySet().iterator();
    for (; exIterator.hasNext(); ) {
      long key = exIterator.next();
      TimeBucket t = (TimeBucket)expiredBuckets.get(key);
      if(startOfBucketsInMillis + (t.bucketKey * noOfBuckets) < time) {
        deleteBucket(t);
        exIterator.remove();
      }
    }
  }

  public void endWindow(long window)
  {
    logger.info("-------------End Window: ----------");
    long maxTime = -1;
    for (int bucketIdx : dirtyBuckets.keySet()) {
      if (maxTimesPerBuckets[bucketIdx] > maxTime) {
        maxTime = maxTimesPerBuckets[bucketIdx];
      }
      maxTimesPerBuckets[bucketIdx] = null;
    }
    if (maxTime > -1) {
      try {
        saveData(window, maxTime);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected void saveData(long window, long id) throws IOException
  {
    Map<Pair<Integer, Long>, Map<Object, T>> dataToStore = Maps.newHashMap();
    long eventsCount = 0;
    for (Map.Entry<Integer, TimeBucket<T>> entry : dirtyBuckets.entrySet()) {
      TimeBucket<T> bucket = entry.getValue();
      dataToStore.put(new Pair<Integer, Long> (entry.getKey(), bucket.bucketKey), ((TimeBucket)bucket).getEvents());
      eventsCount += ((TimeBucket) bucket).getEvents().size();
      bucket.transferDataFromMemoryToStore();
    }
    if (!dataToStore.isEmpty()) {
      long start = System.currentTimeMillis();
      logger.debug("start store {}", window);
      storeBucketData(window, id, dataToStore);
      logger.debug("end store {} num {} took {}", window, eventsCount, System.currentTimeMillis() - start);
    }
    dirtyBuckets.clear();
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

  public void storeBucketData(long window, long timestamp, Map<Pair<Integer, Long>, Map<Object, T>> data) throws IOException
  {
    logger.info("BucketData: {}", bucketRoot);
    FileSystem fs = null;
    FSDataOutputStream dataStream = null;
    TFile.Writer writer = null;

    try {
      long offset = 0;
      for (Map.Entry<Pair<Integer, Long>, Map<Object, T>>  bucketInfo : data.entrySet()) {
        int bucketIdx = bucketInfo.getKey().getFirst();
        if(fs != null) {
          writer.close();
          dataStream.close();
          fs.close();
        }
        Path dataFilePath = new Path(bucketRoot + PATH_SEPARATOR + window + PATH_SEPARATOR + bucketInfo.getKey().getSecond());
        fs = FileSystem.newInstance(dataFilePath.toUri(), configuration);
        dataStream = fs.create(dataFilePath);
        logger.info("data FilePath: {}", dataFilePath.getName());
        int minBlockSize = 64 * 1024;

        String compressName = TFile.COMPRESSION_NONE;

        String comparator = "memcmp";

        setupConfig(fs.getConf());
        writer = new TFile.Writer(dataStream, minBlockSize, compressName, comparator, configuration);

        Map<Object, T> bucketData = bucketInfo.getValue();
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
        for (Map.Entry<Object, T> entry : bucketData.entrySet()) {
          Kryo kryo = new Kryo();
          ByteArrayOutputStream bos = new ByteArrayOutputStream();
          Output output2 = new Output(bos);
          kryo.writeObject(output2, entry.getKey());
          output2.close();
          ByteArrayOutputStream bos1 = new ByteArrayOutputStream();
          Output output1 = new Output(bos1);
          kryo.writeObject(output1, entry.getValue());
          output1.close();
          //sortedData.put(bos.toByteArray(), bos1.toByteArray());

          sortedData.put(bos.toByteArray(), bos1.toByteArray());
          //logger.info("++Key: {} -> {}", entry.getKey(), bos.toByteArray());
          //writer.append(bos.toByteArray(), bos1.toByteArray());
        }
        for(Map.Entry<byte[], byte[]> entry : sortedData.entrySet()) {
          //logger.info("----------Key: {} ", entry.getKey());
          writer.append(entry.getKey(), entry.getValue());
        }

        //logger.info("---------------------------------------------");
        if (bucketPositions[bucketIdx] == null) {
          bucketPositions[bucketIdx] = Maps.newHashMap();
        }
        windowToBuckets.put(window, bucketIdx);
        windowToTimestamp.put(window, timestamp);
        synchronized (bucketPositions[bucketIdx]) {
          bucketPositions[bucketIdx].put(window, offset);
        }
        offset = dataStream.getPos();
      }

    } finally {
      if(fs != null) {
        writer.close();
        dataStream.close();
        fs.close();
      }
    }
  }


  private List<T> getDataFromFile(long window, byte[] keyBytes, long bucketKey) throws IOException
  {
    /*if(window != 0) {
      return null;
    }*/
    Path dataFile = new Path(bucketRoot + PATH_SEPARATOR + window + PATH_SEPARATOR + bucketKey);
    FileSystem fs = FileSystem.newInstance(dataFile.toUri(), configuration);
    if(!fs.exists(dataFile)) {
      return null;
    }
    FSDataInputStream fsdis = fs.open(dataFile);

    DTFile.Reader reader = new DTFile.Reader(fsdis, fs.getFileStatus(dataFile).getLen(), configuration);
    //TFile.Reader reader = new TFile.Reader(fsdis, fs.getFileStatus(dataFile).getLen(), configuration);
    DTFile.Reader.Scanner scanner = reader.createScanner();
    if (scanner.seekTo(keyBytes, 0, keyBytes.length)) {
      DTFile.Reader.Scanner.Entry en = scanner.entry();
      byte[] rkey = new byte[en.getKeyLength()];
      byte[] value = new byte[en.getValueLength()];
      en.getKey(rkey);
      en.getValue(value);
      scanner.advance();
      Input lInput = new Input(value);
      Kryo kro = new Kryo();
      List<T> t = (List<T>)kro.readObject(lInput, ArrayList.class);
      reader.close();
      fsdis.close();
      fs.close();
      return t;
    }
    reader.close();
    fsdis.close();
    fs.close();
    return null;
  }



  private void deleteBucket(TimeBucket bucket) {
    if(bucket == null) {
      return;
    }
    Map<Object, List<T>> writtens = bucket.getEvents();
    if(writtens == null) {
      return;
    }
    for(Map.Entry<Object, List<T>> e: writtens.entrySet()) {
      key2Buckets.get(e.getKey()).remove(bucket.bucketKey);
      if(key2Buckets.get(e.getKey()).size() == 0) {
        key2Buckets.remove(e.getKey());
      }
    }
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
  }
}

