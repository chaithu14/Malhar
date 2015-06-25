package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.google.common.collect.Maps;
import java.util.Timer;
import java.util.TimerTask;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.validation.constraints.Min;
import java.util.Map;
import java.util.Random;
import org.codehaus.jackson.type.TypeReference;

/**
 * Generates sales events data and sends them out as JSON encoded byte arrays.
 * <p>
 * Sales events are JSON string encoded as byte arrays.  All id's are expected to be positive integers, and default to 1.
 * Transaction amounts are double values with two decimal places.  Timestamp is unix epoch in milliseconds.
 * Product categories are not assigned by default.  They are expected to be added by the Enrichment operator, but can be
 * enabled with addProductCategory override.
 *
 * Example Sales Event
 *
 * {
 *    "productId": 1,
 *    "customerId": 12345,
 *    "productCategory": 0,
 *    "regionId": 2,
 *    "channelId": 3,
 *    "amount": 107.99,
 *    "tax": 7.99,
 *    "discount": 15.73,
 *    "timestamp": 1412897574000
 * }
 *
 * @displayName JSON Sales Event Generator
 * @category Input
 * @tags input, generator, json
 *
 * @since 2.0.0
 */
public class JsonProductGenerator implements InputOperator
{
  @Min(1)
  private int maxProductId = 10000000;
  @Min(1)
  private int maxProductCategories = 900;

  // Limit number of emitted tuples per window
  @Min(0)
  private long maxTuplesPerWindow = 40000;

  // Maximum amount of deviation below the maximum tuples per window
  @Min(0)
  private int tuplesPerWindowDeviation = 20000;

  // Number of windows to maintain the same deviation before selecting another
  @Min(1)
  private int tuplesRateCycle = 40;

  /**
   * Outputs sales event in JSON format as a byte array
   */
  //public final transient DefaultOutputPort<byte[]> jsonBytes = new DefaultOutputPort<byte[]>();
  private static final ObjectMapper Objectmapper = new ObjectMapper();
  private static final ObjectReader reader = Objectmapper.reader(new TypeReference<Map<String,Object>>() { });


  private static final ObjectMapper mapper = new ObjectMapper().setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
  private final Random random = new Random();

  private long tuplesCounter = 0;
  private long tuplesPerCurrentWindow = maxTuplesPerWindow;
  private transient Map<Integer, Double> channelDiscount = Maps.newHashMap();
  private transient Map<Integer, Double> regionalDiscount = Maps.newHashMap();
  private transient Map<Integer, Double> regionalTax = Maps.newHashMap();
  private transient RandomWeightedMovableGenerator<Integer> regionalGenerator = new RandomWeightedMovableGenerator<Integer>();
  private transient RandomWeightedMovableGenerator<Integer> channelGenerator = new RandomWeightedMovableGenerator<Integer>();
  public final transient DefaultOutputPort<Map<String, Object>> outputMap = new DefaultOutputPort<Map<String, Object>>();
  public final transient DefaultOutputPort<ProductEvent> outputPort = new DefaultOutputPort<ProductEvent>();
  private transient Timer slidingTimer;
  private long startTime = System.currentTimeMillis();
  private long timeBucket;
  private long timeInterval;

  @Override
  public void beginWindow(long windowId)
  {
    tuplesCounter = 0;
    // Generate new output rate after tuplesRateCycle windows ONLY if tuplesPerWindowDeviation is non-zero
    if (windowId % tuplesRateCycle == 0 && tuplesPerWindowDeviation > 0) {
      tuplesPerCurrentWindow = maxTuplesPerWindow - random.nextInt(tuplesPerWindowDeviation);
    }

  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    startService();
    tuplesPerCurrentWindow = maxTuplesPerWindow;
  }

  @Override
  public void teardown()
  {
    slidingTimer.cancel();
  }

  @Override
  public void emitTuples()
  {
    while (tuplesCounter++ < tuplesPerCurrentWindow) {
      try {

        ProductEvent salesEvent = generateProductEvent();
        if(outputMap.isConnected()) {
          Map<String, Object> tuple = reader.readValue(mapper.writeValueAsBytes(salesEvent));
          this.outputMap.emit(tuple);
        }
        if(outputPort.isConnected()) {
          this.outputPort.emit(salesEvent);
        }

      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }


  public void startService()
  {
    slidingTimer = new Timer();
    slidingTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        synchronized (this) {
          startTime += timeBucket;
        }

      }
    }, timeBucket, timeBucket);
  }

  ProductEvent generateProductEvent() throws Exception {
    ProductEvent salesEvent = new ProductEvent();
    salesEvent.productId = randomId(maxProductId);
    salesEvent.productCategory = 1 + (salesEvent.productId % maxProductCategories);
    synchronized (this) {
      salesEvent.timestamp = startTime + random.nextInt((int) timeInterval);
    }
    return salesEvent;
  }

  private int randomId(int max) {
    // Provide safe default for invalid max
    if (max < 1) return 1;
    return 1 + random.nextInt(max);
  }

  // Generate random tax given transaction amount
  private double randomPercent(double amount, double percent) {
    double tax = amount * ( random.nextDouble() * percent);
    return Math.floor(tax * 100) / 100;
  }


  public long getMaxTuplesPerWindow() {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(long maxTuplesPerWindow) {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  public int getMaxProductId() {
    return maxProductId;
  }

  public void setMaxProductId(int maxProductId) {
    if (maxProductId >= 1)
      this.maxProductId = maxProductId;
  }

  public int getMaxProductCategories() {
    return maxProductCategories;
  }

  public void setMaxProductCategories(int maxProductCategories) {
    this.maxProductCategories = maxProductCategories;
  }

  public int getTuplesPerWindowDeviation() {
    return tuplesPerWindowDeviation;
  }

  public void setTuplesPerWindowDeviation(int tuplesPerWindowDeviation) {
    this.tuplesPerWindowDeviation = tuplesPerWindowDeviation;
  }

  public int getTuplesRateCycle() {
    return tuplesRateCycle;
  }

  public void setTuplesRateCycle(int tuplesRateCycle) {
    this.tuplesRateCycle = tuplesRateCycle;
  }

  public long getTimeInterval()
  {
    return timeInterval;
  }

  public void setTimeInterval(long timeInterval)
  {
    this.timeInterval = timeInterval;
  }

  public long getTimeBucket()
  {
    return timeBucket;
  }

  public void setTimeBucket(long timeBucket)
  {
    this.timeBucket = timeBucket;
  }
}
