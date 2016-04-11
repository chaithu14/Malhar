package com.datatorrent.contrib.dataflow;

import java.util.Collection;

import org.joda.time.Instant;
import org.joda.time.format.PeriodFormat;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.UserCodeException;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingInternals;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

public abstract class AbstractParDoWrapper<IN,OUTDF, OUTFL> implements Operator
{
  public final transient DefaultOutputPort output = new DefaultOutputPort();
  @FieldSerializer.Bind(JavaSerializer.class)
  private DoFn<IN, OUTDF> doFn;
  @FieldSerializer.Bind(JavaSerializer.class)
  private WindowingStrategy<?, ?> windowingStrategy;
  private transient PipelineOptions options;

  private DoFnProcessContext context;

  public AbstractParDoWrapper()
  {

  }

  public AbstractParDoWrapper(PipelineOptions options, WindowingStrategy<?, ?> windowingStrategy, DoFn<IN, OUTDF> doFn) {
    Preconditions.checkNotNull(options);
    Preconditions.checkNotNull(windowingStrategy);
    Preconditions.checkNotNull(doFn);

    this.doFn = doFn;
    this.options = options;
    this.windowingStrategy = windowingStrategy;
  }

  public final transient DefaultInputPort<WindowedValue<IN>> input = new DefaultInputPort<WindowedValue<IN>>()
  {
    @Override
    public void process(WindowedValue<IN> tuple)
    {
      try {
        processElement(tuple);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  };

  private void processElement(WindowedValue<IN> value) throws Exception {
    this.context.setElement(value);
    this.doFn.startBundle(context);
    doFn.processElement(context);
    this.doFn.finishBundle(context);
  }

  @Override
  public void beginWindow(long windowId)
  {

  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (this.context == null) {
      this.context = new DoFnProcessContext(doFn, output);
    }
  }

  @Override
  public void teardown()
  {

  }

  private class DoFnProcessContext extends DoFn<IN,OUTDF>.ProcessContext
  {
    private final DoFn<IN, OUTDF> fn;
    private WindowedValue<IN> ele;
    public final transient DefaultOutputPort outputPort;

    private DoFnProcessContext(DoFn<IN, OUTDF> function, DefaultOutputPort out) {
      function.super();
      super.setupDelegateAggregators();

      this.fn = function;
      this.outputPort = out;
    }

    public void setElement(WindowedValue<IN> value) {
      this.ele = value;
    }

    @Override
    public IN element()
    {
      return this.ele.getValue();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view)
    {
      throw new RuntimeException("sideInput() is not supported in Streaming mode.");
    }

    @Override
    public Instant timestamp()
    {
      return this.ele.getTimestamp();
    }

    @Override
    public BoundedWindow window()
    {
      if (!(fn instanceof DoFn.RequiresWindowAccess)) {
        throw new UnsupportedOperationException(
          "window() is only available in the context of a DoFn marked as RequiresWindow.");
      }

      Collection<? extends BoundedWindow> windows = this.ele.getWindows();
      if (windows.size() != 1) {
        throw new IllegalArgumentException("Each element is expected to belong to 1 window. " +
          "This belongs to " + windows.size() + ".");
      }
      return windows.iterator().next();
    }

    @Override
    public PaneInfo pane()
    {
      return this.ele.getPane();
    }

    @Override
    public WindowingInternals<IN, OUTDF> windowingInternals()
    {
      return windowingInternalsHelper(ele, outputPort);
    }

    @Override
    public PipelineOptions getPipelineOptions()
    {
      return options;
    }

    @Override
    public void output(OUTDF output)
    {
      outputWithTimestamp(output, this.ele.getTimestamp());
    }

    @Override
    public void outputWithTimestamp(OUTDF output, Instant timestamp)
    {
      outputWithTimestampHelper(ele, output, timestamp, outputPort);
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output)
    {
      sideOutputWithTimestamp(tag, output, this.ele.getTimestamp());
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp)
    {
      sideOutputWithTimestampHelper(ele, output, timestamp, tag, outputPort);
    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(String name, Combine.CombineFn<AggInputT, ?, AggOutputT> combiner)
    {
      AggregatorWrapper<AggInputT, AggOutputT> acc = new AggregatorWrapper<>(combiner);
      return acc;
    }
  }

  protected void checkTimestamp(WindowedValue<IN> ref, Instant timestamp) {
    if (timestamp.isBefore(ref.getTimestamp().minus(doFn.getAllowedTimestampSkew()))) {
      throw new IllegalArgumentException(String.format(
        "Cannot output with timestamp %s. Output timestamps must be no earlier than the "
          + "timestamp of the current input (%s) minus the allowed skew (%s). See the "
          + "DoFn#getAllowedTimestmapSkew() Javadoc for details on changing the allowed skew.",
        timestamp, ref.getTimestamp(),
        PeriodFormat.getDefault().print(doFn.getAllowedTimestampSkew().toPeriod())));
    }
  }


  protected <T> WindowedValue<T> makeWindowedValue(
    T output, Instant timestamp, Collection<? extends BoundedWindow> windows, PaneInfo pane) {
    final Instant inputTimestamp = timestamp;
    final WindowFn windowFn = windowingStrategy.getWindowFn();

    if (timestamp == null) {
      timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    if (windows == null) {
      try {
        windows = windowFn.assignWindows(windowFn.new AssignContext() {
          @Override
          public Object element() {
            throw new UnsupportedOperationException(
              "WindowFn attempted to access input element when none was available");
          }

          @Override
          public Instant timestamp() {
            if (inputTimestamp == null) {
              throw new UnsupportedOperationException(
                "WindowFn attempted to access input timestamp when none was available");
            }
            return inputTimestamp;
          }

          @Override
          public Collection<? extends BoundedWindow> windows() {
            throw new UnsupportedOperationException(
              "WindowFn attempted to access input windows when none were available");
          }
        });
      } catch (Exception e) {
        throw UserCodeException.wrap(e);
      }
    }

    return WindowedValue.of(output, timestamp, windows, pane);
  }

  ///////////			ABSTRACT METHODS TO BE IMPLEMENTED BY SUBCLASSES			/////////////////

  public abstract void outputWithTimestampHelper(
    WindowedValue<IN> inElement,
    OUTDF output,
    Instant timestamp, DefaultOutputPort out);

  public abstract <T> void sideOutputWithTimestampHelper(
    WindowedValue<IN> inElement,
    T output,
    Instant timestamp,
    TupleTag<T> tag, DefaultOutputPort out);

  public abstract WindowingInternals<IN, OUTDF> windowingInternalsHelper(
    WindowedValue<IN> inElement, DefaultOutputPort out);

}
